using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using System.Threading.Tasks.Sources;

namespace Ruvio.Client;

internal sealed class CommandMultiplexer
{
    private readonly Stream _stream;
    private readonly RespReader _reader;
    private readonly SemaphoreSlim _capacity;
    private readonly Channel<Request> _outgoing;
    private readonly Channel<Request> _pending;
    private readonly bool _dedicatedReader;
    // 1 while one party owns the read side: a caller reading its own reply (see LeadAsync), the
    // background loop, or the dedicated reader thread.
    private int _readingOwner;
    private Task _receiver;
    private readonly Request?[] _pool;
    private readonly bool _coalesceWrites;
    private int _pooled;
    // Requests in the write path. The 0->1 transition starts the single writer loop, which exits when the count returns to 0.
    private int _scheduled;
    private TaskCompletionSource? _writerStopped;
    private Exception? _failure;
    // RuvioClientOptions.CommandTimeout; zero means no limit.
    private readonly TimeSpan _commandTimeout;
    // The plain limit in Stopwatch ticks; 0 when there is none.
    private readonly long _timeoutTicks;
    // One timer per connection enforces every deadline. The writer records each sent request with a
    // deadline in _sent; the timer fires at the earliest deadline, fails the overdue requests (their
    // late replies are still drained) and re-arms for the next one. Nothing is allocated per command.
    private readonly Timer? _timer;
    private readonly object _timerGate = new();
    // The deadline the timer is set for (long.MaxValue when it is idle). Written under _timerGate.
    private long _armedFor = long.MaxValue;
    // Requests sent with a deadline, in send order: written by the writer only, scanned by the timer.
    // A slot is reused once its request completes; the ring grows (up to capacity) when it is full.
    private Request?[]? _sent;
    private long[]? _sentStamps;
    private int _sentNext;
    private readonly int _sentLimit;
    // When the stream write in progress started (0 when none): a write that never finishes is a dead peer.
    private long _writeStarted;
    // The request whose caller is reading the socket itself, and the token that stops that read when
    // the request expires (see LeadToken).
    private Request? _leading;
    private CancellationTokenSource? _leadCancel;
    // A request whose replies a leader stopped reading at a reply boundary; the next reader goes on.
    private Request? _carried;
    private RespValue[]? _carriedValues;
    private int _carriedIndex;

    // dedicatedReader: the stream blocks (BlockingSocketStream); replies are read by a thread of
    // this multiplexer's own, and writes complete in the caller.
    internal CommandMultiplexer(Stream stream, RespReader reader, int capacity, bool coalesceWrites = true, bool dedicatedReader = false, TimeSpan commandTimeout = default)
    {
        _commandTimeout = commandTimeout;

        if (commandTimeout > TimeSpan.Zero)
        {
            _timeoutTicks = Math.Max(1, (long)(commandTimeout.TotalSeconds * Stopwatch.Frequency));
            _sentLimit = capacity;
            _sent = new Request?[Math.Min(capacity, 16)];
            _sentStamps = new long[_sent.Length];
            _timer = new Timer(static state => ((CommandMultiplexer)state!).OnTimer(), this, Timeout.Infinite, Timeout.Infinite);
        }

        _stream = stream;
        _coalesceWrites = coalesceWrites;
        _reader = reader;
        _capacity = new SemaphoreSlim(capacity, capacity);
        _pool = new Request?[Math.Min(capacity, 1024)];
        // Bounded at the same capacity as _capacity, so a slow socket pushes back on callers instead of growing a queue.
        _outgoing = Channel.CreateBounded<Request>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            AllowSynchronousContinuations = true,
        });

        _pending = Channel.CreateBounded<Request>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true,
        });

        _dedicatedReader = dedicatedReader;

        if (dedicatedReader)
        {
            _readingOwner = 1;
            _receiver = StartReaderThread();
        }
        else
        {
            _receiver = Task.CompletedTask;
        }
    }

    internal bool IsBroken => Volatile.Read(ref _failure) is not null;

    internal ValueTask<IReadOnlyList<RespValue>> SendAsync(
        IReadOnlyList<IReadOnlyList<string>> commands, CancellationToken token)
    {
        Request? request = Admit(commands.Count, token, out Exception? error);

        if (request is null)
            return ValueTask.FromException<IReadOnlyList<RespValue>>(error!);

        request.SetCommands(commands);

        if (_timeoutTicks != 0)
            request.Deadline = DeadlineFor(request.QueuedAt, BlockingWait.OfPipeline(commands));

        bool lead = CanLead(token);

        return LeadOrWait(lead, request, request.SubmitMany(this));
    }

    internal ValueTask<RespValue> SendCommandAsync(in RespCommand command, CancellationToken token)
    {
        Request? request = Admit(1, token, out Exception? error);

        if (request is null)
            return ValueTask.FromException<RespValue>(error!);

        request.SetCommand(command);

        if (_timeoutTicks != 0)
            request.Deadline = DeadlineFor(request.QueuedAt, BlockingWait.OfCommand(command));

        bool lead = CanLead(token);

        return LeadOrWait(lead, request, request.SubmitOne(this));
    }

    internal ValueTask<RespValue> SendAsync(IReadOnlyList<ReadOnlyMemory<byte>> arguments, CancellationToken token)
    {
        Request? request = Admit(1, token, out Exception? error);

        if (request is null)
            return ValueTask.FromException<RespValue>(error!);

        request.SetArguments(arguments);

        if (_timeoutTicks != 0)
            request.Deadline = DeadlineFor(request.QueuedAt, BlockingWait.OfArguments(arguments));

        bool lead = CanLead(token);

        return LeadOrWait(lead, request, request.SubmitOne(this));
    }

    internal ValueTask<IReadOnlyList<RespValue>> SendAsync(
        ReadOnlyMemory<byte> payload, int replies, CancellationToken token)
    {
        Request? request = Admit(replies, token, out Exception? error);

        if (request is null)
            return ValueTask.FromException<IReadOnlyList<RespValue>>(error!);

        request.SetPayload(payload);

        if (_timeoutTicks != 0)
            request.Deadline = DeadlineFor(request.QueuedAt, BlockingWait.OfPayload(payload.Span));

        bool lead = CanLead(token);

        return LeadOrWait(lead, request, request.SubmitMany(this));
    }

    /// <summary>
    /// When a send must be answered, in <see cref="Stopwatch"/> ticks: <c>CommandTimeout</c> from
    /// <paramref name="start"/>, plus a blocking command's own wait (see <see cref="BlockingWait"/>);
    /// 0 when it may wait forever.
    /// </summary>
    private long DeadlineFor(long start, TimeSpan wait)
    {
        if (wait == TimeSpan.Zero)
            return start + _timeoutTicks;

        if (wait == BlockingWait.Forever)
            return 0;

        double total = _commandTimeout.TotalSeconds + wait.TotalSeconds;

        // Past about 24 days the limit is no limit, as it was when this used CancelAfter.
        return total * 1000 >= int.MaxValue - 1 ? 0 : start + (long)(total * Stopwatch.Frequency);
    }

    /// <summary>
    /// Records a request that is about to enter the reply queue, so the timer can find it. The writer
    /// calls it after the request is encoded: from then on, failing the caller cannot let the writer
    /// read memory the caller has reused.
    /// </summary>
    private void Track(Request request)
    {
        Request?[] ring = _sent!;
        long[] stamps = _sentStamps!;
        int slot = _sentNext;
        Request? previous = ring[slot];

        // The slot's request still waits for its reply: make room rather than lose it.
        if (previous is not null && (stamps[slot] & 1) == 0 && previous.Stamp == stamps[slot] && ring.Length < _sentLimit)
        {
            int size = (int)Math.Min((long)ring.Length * 2, _sentLimit);
            var grown = new Request?[size];
            var grownStamps = new long[size];

            for (int index = 0; index < ring.Length; index++)
            {
                int from = (slot + index) % ring.Length;
                grown[index] = ring[from];
                grownStamps[index] = stamps[from];
            }

            slot = ring.Length;
            ring = grown;
            stamps = grownStamps;
            _sentStamps = grownStamps;
            Volatile.Write(ref _sent, grown);
        }

        long stamp = request.Stamp;

        Volatile.Write(ref request.QueuedStamp, stamp);
        stamps[slot] = stamp;
        // A full fence: the timer sets _armedFor before it scans, this sets the slot before it reads
        // _armedFor, so either the scan sees the request or this call arms the timer for it.
        Interlocked.Exchange(ref ring[slot], request);
        _sentNext = slot + 1 == ring.Length ? 0 : slot + 1;

        if (request.Deadline < Volatile.Read(ref _armedFor))
            Arm(request.Deadline);
    }

    private void Arm(long deadline)
    {
        lock (_timerGate)
        {
            if (deadline >= _armedFor || _timer is null || IsBroken)
                return;

            Volatile.Write(ref _armedFor, deadline);
            long ticks = deadline - Stopwatch.GetTimestamp();
            long due = ticks <= 0 ? 0 : (long)Math.Ceiling(ticks * 1000.0 / Stopwatch.Frequency);

            _timer.Change(Math.Min(due, int.MaxValue - 1), Timeout.Infinite);
        }
    }

    /// <summary>
    /// The timer: fails each sent request whose deadline has passed with a <see cref="TimeoutException"/>.
    /// Like a cancelled wait, the request's late reply is still read and dropped, so the connection stays
    /// in use. A stream write that has not finished within the command timeout means the peer stopped
    /// reading; then the connection is closed. Re-arms for the next deadline, if any.
    /// </summary>
    private void OnTimer()
    {
        lock (_timerGate)
        {
            if (IsBroken)
                return;

            Volatile.Write(ref _armedFor, long.MaxValue);
        }

        Interlocked.MemoryBarrier();
        long now = Stopwatch.GetTimestamp();
        long next = long.MaxValue;
        Request?[] ring = Volatile.Read(ref _sent)!;

        foreach (Request? request in ring)
        {
            if (request is null)
                continue;

            long stamp = Volatile.Read(ref request.QueuedStamp);

            // Completed, or recycled for a command that is not sent yet.
            if ((stamp & 1) != 0 || request.Stamp != stamp)
                continue;

            long deadline = Volatile.Read(ref request.Deadline);

            if (deadline == 0)
                continue;

            if (deadline > now)
            {
                next = Math.Min(next, deadline);

                continue;
            }

            TimeSpan limit = TimeSpan.FromSeconds((deadline - request.QueuedAt) / (double)Stopwatch.Frequency);

            if (request.TryExpire(stamp, limit) && ReferenceEquals(request, Volatile.Read(ref _leading)))
                Interlocked.Exchange(ref _leadCancel, null)?.Cancel();
        }

        long writing = Volatile.Read(ref _writeStarted);

        if (writing != 0)
        {
            if (now - writing >= _timeoutTicks)
            {
                Fail(new TimeoutException($"ruvio: the socket took no data within the command timeout of {_commandTimeout}"));

                return;
            }

            next = Math.Min(next, writing + _timeoutTicks);
        }

        if (next != long.MaxValue)
            Arm(next);
    }

    private void StopTimer()
    {
        if (_timer is null)
            return;

        lock (_timerGate)
            _timer.Dispose();
    }

    /// <summary>
    /// The token a leader reads with: cancelled when <paramref name="mine"/> expires, so its caller is
    /// not held by a read that may never finish. <c>default</c> when the request has no deadline.
    /// </summary>
    private CancellationToken LeadToken(Request mine)
    {
        if (mine.Deadline == 0)
            return default;

        // A full fence: the timer completes the request before it reads _leading.
        Interlocked.Exchange(ref _leading, mine);
        CancellationTokenSource? cancel = Volatile.Read(ref _leadCancel);

        if (cancel is null)
        {
            var created = new CancellationTokenSource();

            cancel = Interlocked.CompareExchange(ref _leadCancel, created, null) ?? created;
        }

        return cancel.Token;
    }

    /// <summary>
    /// When nobody is reading, a caller whose wait cannot be cancelled reads the socket itself
    /// until its own reply is in, completing the requests queued before it on the way. Its reply
    /// then needs no hand-off from a reader to the thread pool. Others wait for the reader.
    /// </summary>
    // Taken before the request is submitted, so the writer does not start the background reader for it.
    private bool CanLead(CancellationToken token) => !_dedicatedReader && !token.CanBeCanceled && TryTakeReading();

    private ValueTask<T> LeadOrWait<T>(bool lead, Request request, ValueTask<T> pending)
    {
        if (!lead)
            return pending;

        if (pending.IsCompleted)
        {
            ReleaseReading();

            return pending;
        }

        return LeadAsync(request, pending);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<T> LeadAsync<T>(Request mine, ValueTask<T> pending)
    {
        await ReadUntilAsync(mine).ConfigureAwait(false);

        return await pending.ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ReadUntilAsync(Request mine)
    {
        CancellationToken lead = LeadToken(mine);

        try
        {
            while (!mine.IsCompleted)
            {
                if (TryNextReply(out Request? request, out RespValue[]? values, out int index))
                {
                    if (await ReadRepliesAsync(request!, values, index, lead).ConfigureAwait(false) != ReplyRead.Done)
                        break;

                    continue;
                }

                // Its request is not in the reply queue yet: another caller is writing, it waits for
                // capacity, or it failed before it was sent. Give up the read side and wait like any
                // caller; the writer starts a reader when the request is queued.
                break;
            }
        }
        finally
        {
            if (lead.CanBeCanceled)
                Volatile.Write(ref _leading, null);

            ReleaseReading();
        }
    }

    /// <summary>The next request to read replies for: one a leader handed over, else the oldest queued.</summary>
    private bool TryNextReply(out Request? request, out RespValue[]? values, out int index)
    {
        request = _carried;

        if (request is not null)
        {
            values = _carriedValues;
            index = _carriedIndex;
            _carried = null;
            _carriedValues = null;

            return true;
        }

        values = null;
        index = 0;

        return _pending.Reader.TryRead(out request);
    }

    private bool HasReplies => Volatile.Read(ref _carried) is not null || _pending.Reader.TryPeek(out _);

    private bool TryTakeReading() => Interlocked.CompareExchange(ref _readingOwner, 1, 0) == 0;

    /// <summary>Gives up the read side; requests queued meanwhile get the background reader.</summary>
    private void ReleaseReading()
    {
        Volatile.Write(ref _readingOwner, 0);

        if (HasReplies)
            EnsureReader();
    }

    /// <summary>Called after a request enters the reply queue: starts the background reader unless someone reads.</summary>
    private void EnsureReader()
    {
        if (TryTakeReading())
            _receiver = ReadLoopAsync();
    }

    private Request? Admit(int replies, CancellationToken token, out Exception? error)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(replies);
        error = token.IsCancellationRequested ? new OperationCanceledException(token)
            : IsBroken ? Failure() : null;

        if (error is not null)
            return null;

        Request request = Rent();
        request.Initialize(replies, token);

        return request;
    }

    /// <summary>
    /// Publishes an admitted request. Returns false when the request was not published;
    /// the caller then completes it with <paramref name="error"/> and the request was never seen by the pipeline.
    /// </summary>
    private bool TryPublish(Request request, out Exception? error)
    {
        error = null;

        if (!_capacity.Wait(0))
            return false;

        if (!PublishAdmitted(request, out error))
        {
            request.DisposePayload();
            _capacity.Release();

            return false;
        }

        return true;
    }

    private bool PublishAdmitted(Request request, out Exception? error)
    {
        error = null;
        try
        {
            ThrowIfBroken();
            request.PreparePayload();
        }
        catch (Exception exception)
        {
            error = exception;

            return false;
        }

        // Reserve ownership before publishing: the active writer cannot go idle
        // while another producer is about to enqueue its command.
        if (Interlocked.Increment(ref _scheduled) == 1)
        {
            _ = WriteLoopAsync(request);

            return true;
        }

        if (_outgoing.Writer.TryWrite(request))
            return true;

        WriterFinished();
        error = IsBroken ? Failure() : new InvalidOperationException("Admitted request exceeded the writer queue capacity.");

        return false;
    }

    /// <summary>Slow path when all in-flight slots are taken: wait for a slot, then publish.</summary>
    private async ValueTask PublishWhenAvailableAsync(Request request)
    {
        bool published = false;
        try
        {
            if (request.Deadline == 0)
                await _capacity.WaitAsync(request.Token).ConfigureAwait(false);
            else
            {
                // The slow path only: a wait for a slot is bounded by the command's deadline as well.
                long left = request.Deadline - Stopwatch.GetTimestamp();
                int millis = left <= 0 ? 0 : (int)Math.Min(int.MaxValue - 1, Math.Ceiling(left * 1000.0 / Stopwatch.Frequency));

                if (!await _capacity.WaitAsync(millis, request.Token).ConfigureAwait(false))
                {
                    TimeSpan limit = TimeSpan.FromSeconds((request.Deadline - request.QueuedAt) / (double)Stopwatch.Frequency);

                    throw new TimeoutException($"ruvio: no reply within the command timeout of {limit}");
                }
            }

            if (!PublishAdmitted(request, out Exception? error))
            {
                _capacity.Release();
                throw error!;
            }

            published = true;
        }
        catch (Exception error) when (!published)
        {
            request.DisposePayload();
            request.FailUnpublished(error is OperationCanceledException && request.Token.IsCancellationRequested
                ? new OperationCanceledException(request.Token)
                : error);

            return;
        }

    }

    private Request Rent()
    {
        lock (_pool)
        {
            if (_pooled > 0)
            {
                Request request = _pool[--_pooled]!;
                _pool[_pooled] = null;

                return request;
            }
        }

        return new Request();
    }

    private void Return(Request request)
    {
        request.Reset();
        lock (_pool)
        {
            if (_pooled < _pool.Length)
                _pool[_pooled++] = request;
        }
    }

    private async Task WriteLoopAsync(Request first)
    {
        Request? current = first;
        bool pending = false;

        try
        {
            while (current is not null)
            {
                Request request = current;
                pending = false;
                ThrowIfBroken();

                if (request.Token.IsCancellationRequested)
                {
                    request.TryCancel();
                    request.DisposePayload();
                    Finish(request);
                    current = null;
                    current = await NextWriteAsync().ConfigureAwait(false);
                    continue;
                }

                PooledFrame encoded;

                try
                {
                    long encodeStarted = RuvioSocket.PhaseMetricsEnabled ? Stopwatch.GetTimestamp() : 0;
                    encoded = request.Encode();

                    if (encodeStarted != 0)
                        RuvioSocket.RecordSocketPhase("encode", encodeStarted);
                }
                catch (Exception error)
                {
                    request.TryFail(error);
                    request.DisposePayload();
                    Finish(request);
                    current = null;
                    current = await NextWriteAsync().ConfigureAwait(false);

                    continue;
                }

                using PooledFrame frame = encoded;
                using (var activity = RuvioSocket.StartPhase("queue", request.Parent, request.QueuedAt))
                    RuvioSocket.RecordSocketPhase("queue", request.QueuedAt);

                request.SentAt = Stopwatch.GetTimestamp();
                // The reader may complete and recycle the request once it is pending.
                Activity? parent = request.Parent;

                if (request.Deadline != 0)
                    Track(request);

                await _pending.Writer.WriteAsync(request).ConfigureAwait(false);
                pending = true;
                EnsureReader();
                long started = Stopwatch.GetTimestamp();

                using (var activity = RuvioSocket.StartPhase("write", parent))
                {
                    Volatile.Write(ref _writeStarted, started);

                    try
                    {
                        if (_coalesceWrites && frame.Length < 64 * 1024 && _outgoing.Reader.TryRead(out Request? next))
                            await WriteQueuedBatchAsync(frame, next).ConfigureAwait(false);
                        else
                        {
                            await _stream.WriteAsync(frame.Buffer.AsMemory(0, frame.Length)).ConfigureAwait(false);
                            RuvioSocket.RecordSocketWrite(frame.Length);
                        }
                    }
                    finally
                    {
                        Volatile.Write(ref _writeStarted, 0);
                        RuvioSocket.RecordSocketPhase("write", started);
                    }
                }

                current = null;
                current = await NextWriteAsync().ConfigureAwait(false);
            }
        }
        catch (Exception error)
        {
            Fail(error);

            if (current is not null && !pending)
            {
                current.TryFail(Failure(closedBeforeReply: true));
                current.DisposePayload();
                Finish(current);
            }

            if (current is not null)
                WriterFinished();

            while (_outgoing.Reader.TryRead(out Request? request))
            {
                request.TryFail(Failure(closedBeforeReply: true));
                request.DisposePayload();
                Finish(request);
                WriterFinished();
            }
        }
    }

    private async Task WriteQueuedBatchAsync(PooledFrame first, Request next)
    {
        byte[]? buffer = null;
        int length = first.Length;
        Request? current = next;
        int consumed = 1;
        try
        {
            buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
            first.Buffer.AsSpan(0, length).CopyTo(buffer);

            while (current is not null)
            {
                Request request = current;
                ThrowIfBroken();

                if (request.Token.IsCancellationRequested)
                {
                    request.TryCancel();
                    request.DisposePayload();
                    Finish(request);
                    current = null;
                }
                else
                {
                    PooledFrame frame;
                    try
                    {
                        long started = RuvioSocket.PhaseMetricsEnabled ? Stopwatch.GetTimestamp() : 0;
                        frame = request.Encode();

                        if (started != 0)
                            RuvioSocket.RecordSocketPhase("encode", started);
                    }
                    catch (Exception error)
                    {
                        request.TryFail(error);
                        request.DisposePayload();
                        Finish(request);
                        current = null;

                        if (consumed < 15 && _outgoing.Reader.TryRead(out current))
                            consumed++;

                        continue;
                    }

                    using (frame)
                    {
                        int required = checked(length + frame.Length);

                        if (required > buffer.Length)
                        {
                            byte[] larger = ArrayPool<byte>.Shared.Rent(required);
                            buffer.AsSpan(0, length).CopyTo(larger);
                            ArrayPool<byte>.Shared.Return(buffer);
                            buffer = larger;
                        }

                        frame.Buffer.AsSpan(0, frame.Length).CopyTo(buffer.AsSpan(length));
                        request.SentAt = Stopwatch.GetTimestamp();
                        RuvioSocket.RecordSocketPhase("queue", request.QueuedAt);

                        if (request.Deadline != 0)
                            Track(request);

                        await _pending.Writer.WriteAsync(request).ConfigureAwait(false);
                        EnsureReader();
                        // Do not touch a published request: the reader can recycle it.
                        current = null;
                        length = required;
                    }
                }

                if (consumed < 15 && length < 64 * 1024 && _outgoing.Reader.TryRead(out current))
                    consumed++;
            }

            await _stream.WriteAsync(buffer.AsMemory(0, length)).ConfigureAwait(false);
            RuvioSocket.RecordSocketWrite(length);
        }
        catch (Exception error)
        {
            Fail(error);

            if (current is not null)
            {
                current.TryFail(Failure(closedBeforeReply: true));
                current.DisposePayload();
                Finish(current);
            }

            throw;
        }
        finally
        {
            if (buffer is not null)
                ArrayPool<byte>.Shared.Return(buffer);

            for (int index = 0; index < consumed; index++)
                WriterFinished();
        }
    }

    private ValueTask<Request?> NextWriteAsync()
    {
        if (WriterFinished() == 0)
            return new ValueTask<Request?>((Request?)null);

        if (_outgoing.Reader.TryRead(out Request? next))
            return new ValueTask<Request?>(next);

        return WaitForWriteAsync();
    }

    private async ValueTask<Request?> WaitForWriteAsync() =>
        await _outgoing.Reader.ReadAsync().ConfigureAwait(false);

    private int WriterFinished()
    {
        int remaining = Interlocked.Decrement(ref _scheduled);

        if (remaining == 0 && IsBroken)
            Volatile.Read(ref _writerStopped)?.TrySetResult();

        return remaining;
    }

    /// <summary>The background reader: drains the reply queue, then gives up the read side.</summary>
    private async Task ReadLoopAsync()
    {
        while (true)
        {
            while (TryNextReply(out Request? request, out RespValue[]? values, out int index))
            {
                if (await ReadRepliesAsync(request!, values, index, CancellationToken.None).ConfigureAwait(false) == ReplyRead.Failed)
                {
                    Volatile.Write(ref _readingOwner, 0);

                    return;
                }
            }

            Volatile.Write(ref _readingOwner, 0);

            // A request queued after the last TryRead found the read side taken; take it back.
            if (!HasReplies || !TryTakeReading())
                return;
        }
    }

    private enum ReplyRead
    {
        Done,
        Failed,
        // A leader's read was stopped between two replies; the request waits in _carried.
        HandedOver,
    }

    /// <summary>
    /// Reads one request's replies, from reply <paramref name="index"/> on, and completes it. Failed
    /// when the connection failed: then the request and everything queued behind it have been failed.
    /// A leader passes <paramref name="lead"/>: when it is cancelled while no byte of the next reply has
    /// been read, the request is handed to the next reader and the leader's caller is let go.
    /// </summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<ReplyRead> ReadRepliesAsync(Request request, RespValue[]? values, int index, CancellationToken lead)
    {
        bool replyStarted = index > 0;

        try
        {
            _reader.PrepareReply();
            ThrowIfBroken();

            using (var activity = RuvioSocket.StartPhase("reply", request.Parent, request.SentAt))
            {
                try
                {
                    if (request.Replies == 1)
                    {
                        if (lead.CanBeCanceled && !_reader.HasBuffered && !await WaitForReplyAsync(lead).ConfigureAwait(false))
                            return HandOver(request, null, 0);

                        RespValue value = await _reader.ReadAsync(lead).ConfigureAwait(false);
                        replyStarted = true;
                        request.TrySetResult(value);
                    }
                    else
                    {
                        values ??= new RespValue[request.Replies];

                        for (int i = index; i < values.Length; i++)
                        {
                            if (lead.CanBeCanceled && !_reader.HasBuffered && !await WaitForReplyAsync(lead).ConfigureAwait(false))
                                return HandOver(request, values, i);

                            values[i] = await _reader.ReadAsync(lead).ConfigureAwait(false);
                            replyStarted = true;
                        }

                        request.TrySetResult(values);
                    }
                }
                finally
                {
                    RuvioSocket.RecordSocketPhase("reply", request.SentAt);
                }
            }

            Finish(request);

            return ReplyRead.Done;
        }
        catch (Exception exception)
        {
            // Stopped in the middle of a reply: the parser cannot resume there, so the connection goes.
            Exception error = exception is OperationCanceledException && lead.IsCancellationRequested
                ? new TimeoutException($"ruvio: a reply stalled past the command timeout of {_commandTimeout}")
                : exception;

            // ReplyStarted belongs to this request, never to another concurrent caller.
            bool closedBeforeReply = !replyStarted && !_reader.ReplyStarted;
            Fail(error);
            request.TryFail(new RuvioException(error.Message, false, error, closedBeforeReply));
            Finish(request);

            while (_pending.Reader.TryRead(out Request? queued))
            {
                queued.TryFail(Failure(closedBeforeReply: true));
                Finish(queued);
            }

            return ReplyRead.Failed;
        }
    }

    /// <summary>Waits for the next reply's first byte; false when <paramref name="lead"/> stopped the wait first.</summary>
    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<bool> WaitForReplyAsync(CancellationToken lead)
    {
        try
        {
            await _reader.WaitForDataAsync(lead).ConfigureAwait(false);

            return true;
        }
        catch (OperationCanceledException) when (lead.IsCancellationRequested)
        {
            return false;
        }
    }

    private ReplyRead HandOver(Request request, RespValue[]? values, int index)
    {
        _carriedValues = values;
        _carriedIndex = index;
        Volatile.Write(ref _carried, request);

        return ReplyRead.HandedOver;
    }

    private Task StartReaderThread()
    {
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                ReadLoopBlocking();
            }
            finally
            {
                stopped.TrySetResult();
            }
        })
        {
            IsBackground = true,
            Name = "Ruvio reader",
        };

        thread.Start();

        return stopped.Task;
    }

    /// <summary>
    /// The reader thread: blocks in the socket read until a reply arrives, then completes the
    /// oldest pending request, which the writer queued before its frame went out. Callers resume
    /// on the thread pool, so a caller never runs on this thread.
    /// </summary>
    private void ReadLoopBlocking()
    {
        Request? current = null;
        bool replyStarted = false;

        try
        {
            while (true)
            {
                _reader.PrepareReply();
                replyStarted = false;
                RespValue first = Wait(_reader.ReadAsync(CancellationToken.None));

                replyStarted = true;
                ThrowIfBroken();

                // The writer queues a request before its frame goes out, so it is normally here
                // already; a peer that answers early waits for it.
                while (!_pending.Reader.TryRead(out current))
                {
                    if (!_pending.Reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
                        throw Failure();
                }

                try
                {
                    if (current.Replies == 1)
                    {
                        current.TrySetResult(first);
                    }
                    else
                    {
                        var values = new RespValue[current.Replies];

                        values[0] = first;

                        for (int i = 1; i < values.Length; i++)
                            values[i] = Wait(_reader.ReadAsync(CancellationToken.None));

                        current.TrySetResult(values);
                    }
                }
                finally
                {
                    RuvioSocket.RecordSocketPhase("reply", current.SentAt);
                }

                Finish(current);
                current = null;
            }
        }
        catch (Exception error)
        {
            // A failure before the next reply's first byte belongs to the oldest pending request.
            if (current is null && _pending.Reader.TryRead(out Request? oldest))
            {
                current = oldest;
                replyStarted = false;
            }

            bool closedBeforeReply = current is not null && !replyStarted && !_reader.ReplyStarted;
            Fail(error);

            if (current is not null)
            {
                current.TryFail(new RuvioException(
                    error.Message, false, error, closedBeforeReply));
                Finish(current);
            }
        }
        finally
        {
            while (_pending.Reader.TryRead(out Request? request))
            {
                request.TryFail(Failure(closedBeforeReply: true));
                Finish(request);
            }
        }
    }

    // The blocking stream completes every read synchronously; anything else waits here.
    private static RespValue Wait(ValueTask<RespValue> pending) =>
        pending.IsCompletedSuccessfully ? pending.Result : pending.AsTask().GetAwaiter().GetResult();

    private void Finish(Request request)
    {
        if (Interlocked.Exchange(ref request.Finished, 1) != 0)
            return;

        _capacity.Release();
        request.Release();
    }

    private RuvioException Failure(bool closedBeforeReply = false) => new(
        Volatile.Read(ref _failure)?.Message ?? "connection is closed",
        false, Volatile.Read(ref _failure), closedBeforeReply);

    private void ThrowIfBroken()
    {
        if (IsBroken)
            throw Failure();
    }

    private void Fail(Exception error)
    {
        if (Interlocked.CompareExchange(ref _failure, error, null) is not null)
            return;

        _outgoing.Writer.TryComplete();
        _pending.Writer.TryComplete();
        _stream.Dispose();
        StopTimer();
    }

    internal async Task StopAsync()
    {
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.CompareExchange(ref _writerStopped, stopped, null);
        Fail(new RuvioException("connection is closed"));

        if (Volatile.Read(ref _scheduled) == 0)
            _writerStopped.TrySetResult();

        await Task.WhenAll(_writerStopped.Task, _receiver).ConfigureAwait(false);

        // A caller reading its own reply sees the closed stream and gives up the read side.
        while (!_dedicatedReader && Volatile.Read(ref _readingOwner) != 0)
            await Task.Delay(1).ConfigureAwait(false);

        // Requests that reached the reply queue after the last reader left.
        while (TryNextReply(out Request? request, out _, out _))
        {
            request!.TryFail(Failure(closedBeforeReply: true));
            Finish(request);
        }

        _reader.Dispose();
    }

    /// <summary>
    /// One pooled in-flight command and its completion. Two parties hold a reference:
    /// the caller (released by GetResult) and the pipeline (released by Finish).
    /// The instance is recycled only after both let go, so a cancelled caller cannot
    /// observe another command's reply while its own reply is still being drained.
    /// </summary>
    private sealed class Request : IValueTaskSource<RespValue>, IValueTaskSource<IReadOnlyList<RespValue>>
    {
        private static readonly Action<object?> CancelCallback = static state => ((Request)state!).TryCancel();

        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
        private CommandMultiplexer? _owner;
        private CancellationTokenRegistration _registration;
        private RespValue? _single;
        private IReadOnlyList<RespValue>? _many;
        // The generation (bits 1 and up, bumped by Reset) and whether it completed (bit 0). The timer
        // completes a request only for the generation it saw queued, never a recycled one.
        private long _state = 2;
        private int _callerReleased;
        private int _references;
        private IReadOnlyList<IReadOnlyList<string>>? _commands;
        private RespCommand _command;
        private bool _hasCommand;
        private IReadOnlyList<ReadOnlyMemory<byte>>? _arguments;
        private ReadOnlyMemory<byte> _payload;
        private bool _hasPayload;
        private PooledFrame? _ownedFrame;
        internal int Replies;
        internal CancellationToken Token;
        internal long QueuedAt;
        internal long SentAt;
        // When the reply is due, in Stopwatch ticks; 0 for none.
        internal long Deadline;
        // The Stamp this request had when the writer recorded it for the timer.
        internal long QueuedStamp;
        internal Activity? Parent;
        // Timeout, cancel and the reply can race; only the first Finish call completes the request and releases capacity.
        internal int Finished;

        internal void Initialize(int replies, CancellationToken token)
        {
            Replies = replies;
            Token = token;
            QueuedAt = Stopwatch.GetTimestamp();
            Parent = RuvioSocket.TracingEnabled ? Activity.Current : null;
        }

        internal void SetCommands(IReadOnlyList<IReadOnlyList<string>> commands) => _commands = commands;

        internal void SetCommand(in RespCommand command)
        {
            _command = command;
            _hasCommand = true;
        }

        internal void SetArguments(IReadOnlyList<ReadOnlyMemory<byte>> arguments) => _arguments = arguments;

        internal void SetPayload(ReadOnlyMemory<byte> payload)
        {
            _payload = payload;
            _hasPayload = true;
        }

        internal ValueTask<RespValue> SubmitOne(CommandMultiplexer owner)
        {
            Exception? error = Submit(owner);

            return error is null
                ? new ValueTask<RespValue>(this, _core.Version)
                : ValueTask.FromException<RespValue>(error);
        }

        internal ValueTask<IReadOnlyList<RespValue>> SubmitMany(CommandMultiplexer owner)
        {
            Exception? error = Submit(owner);

            return error is null
                ? new ValueTask<IReadOnlyList<RespValue>>(this, _core.Version)
                : ValueTask.FromException<IReadOnlyList<RespValue>>(error);
        }

        private Exception? Submit(CommandMultiplexer owner)
        {
            _owner = owner;
            _references = 2;

            // Cancellation only abandons the caller's wait. Sent replies must still be drained.
            if (Token.CanBeCanceled)
                _registration = Token.UnsafeRegister(CancelCallback, this);

            if (owner.TryPublish(this, out Exception? error))
                return null;

            if (error is not null)
            {
                // Never published and never handed to the caller: recycle immediately.
                _registration.Dispose();
                DisposePayload();
                owner.Return(this);

                return error;
            }

            _ = owner.PublishWhenAvailableAsync(this);

            return null;
        }

        internal void FailUnpublished(Exception error)
        {
            TryFail(error);
            Release();
        }

        internal long Stamp => Volatile.Read(ref _state);

        private bool TryComplete()
        {
            long state = Volatile.Read(ref _state);

            return (state & 1) == 0 && Interlocked.CompareExchange(ref _state, state | 1, state) == state;
        }

        /// <summary>Fails the request with a timeout if it is still generation <paramref name="stamp"/> and not completed.</summary>
        internal bool TryExpire(long stamp, TimeSpan limit)
        {
            if ((stamp & 1) != 0 || Interlocked.CompareExchange(ref _state, stamp | 1, stamp) != stamp)
                return false;

            _core.SetException(new TimeoutException($"ruvio: no reply within the command timeout of {limit}"));

            return true;
        }

        internal bool TrySetResult(RespValue value)
        {
            if (!TryComplete())
                return false;

            _single = value;
            _core.SetResult(true);

            return true;
        }

        internal bool TrySetResult(IReadOnlyList<RespValue> values)
        {
            if (!TryComplete())
                return false;

            _many = values;
            _core.SetResult(true);

            return true;
        }

        internal bool TryFail(Exception error)
        {
            if (!TryComplete())
                return false;

            _core.SetException(error);

            return true;
        }

        internal bool TryCancel() => TryFail(new OperationCanceledException(Token));

        internal bool IsCompleted => (Volatile.Read(ref _state) & 1) != 0;

        internal void Release()
        {
            if (Interlocked.Decrement(ref _references) == 0)
                _owner!.Return(this);
        }

        internal void Reset()
        {
            _core.Reset();
            _registration = default;
            _single = null;
            _many = null;
            _callerReleased = 0;
            Finished = 0;
            Token = default;
            Parent = null;
            _owner = null;
            DisposePayload();
            Deadline = 0;
            // Last: a new generation, not completed. The timer reads Deadline only after the stamp.
            Volatile.Write(ref _state, (_state | 1) + 1);
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token,
            ValueTaskSourceOnCompletedFlags flags) => _core.OnCompleted(continuation, state, token, flags);

        RespValue IValueTaskSource<RespValue>.GetResult(short token)
        {
            ClaimCaller(token);
            try
            {
                _core.GetResult(token);

                return _single!;
            }
            finally
            {
                ReleaseCaller();
            }
        }

        IReadOnlyList<RespValue> IValueTaskSource<IReadOnlyList<RespValue>>.GetResult(short token)
        {
            ClaimCaller(token);
            try
            {
                _core.GetResult(token);

                // The reader stores one reply unboxed; list callers with one command still get a list.
                return _many ?? [_single!];
            }
            finally
            {
                ReleaseCaller();
            }
        }

        private void ClaimCaller(short token)
        {
            // A second await must fail before touching the reference count, or the request
            // could be recycled while the pipeline still owns it.
            if (token != _core.Version || Interlocked.Exchange(ref _callerReleased, 1) != 0)
            {
                throw new InvalidOperationException("A Ruvio ValueTask can be awaited only once.");
            }
        }

        private void ReleaseCaller()
        {
            _registration.Dispose();
            Release();
        }

        internal void PreparePayload()
        {
            // Cancellation may let a caller reuse binary/raw memory before the writer gets to it.
            if (_hasCommand && _command.IsCallerList && Token.CanBeCanceled)
            {
                _ownedFrame = _command.EncodePooled();
                _command = default;
                _hasCommand = false;
            }
            else if (_commands is not null && Token.CanBeCanceled)
            {
                _ownedFrame = RespCodec.EncodeManyPooled(_commands);
                _commands = null;
            }
            else if (_arguments is not null && Token.CanBeCanceled)
            {
                _ownedFrame = RespCodec.EncodeBinaryPooled(_arguments);
                _arguments = null;
            }
            else if (_hasPayload)
            {
                _ownedFrame = RespCodec.CopyPooled(_payload.Span);
                _payload = default;
                _hasPayload = false;
            }
        }

        internal PooledFrame Encode()
        {
            if (_ownedFrame is PooledFrame frame)
            {
                _ownedFrame = null;

                return frame;
            }

            PooledFrame result = _hasCommand ? _command.EncodePooled()
                : _commands is not null ? RespCodec.EncodeManyPooled(_commands)
                : _arguments is not null ? RespCodec.EncodeBinaryPooled(_arguments)
                : throw new InvalidOperationException("Request payload was already consumed.");

            _commands = null;
            _command = default;
            _hasCommand = false;
            _arguments = null;

            return result;
        }

        // Returns the pooled frame and drops caller memory references, so a buffer is never recycled
        // while the pipeline might still read it.
        internal void DisposePayload()
        {
            _ownedFrame?.Dispose();
            _ownedFrame = null;
            _commands = null;
            _command = default;
            _hasCommand = false;
            _arguments = null;
            _payload = default;
            _hasPayload = false;
        }
    }
}
