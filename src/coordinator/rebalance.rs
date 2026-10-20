//! `COORDINATOR REBALANCE [DRYRUN]`: evens out the hash slots across the groups, in
//! proportion to their shard counts, with a sequence of ordinary moves. The plan is worked
//! out from the committed moves alone; the leader runs it one move at a time and works it
//! out again after each one, so running it again after a stop carries on from where the
//! slots are.
//!
//! The steps are not in the Raft state: a new leader finishes the move in flight, and the
//! rest waits for the next `COORDINATOR REBALANCE`.

use std::time::Duration;

use tracing::{info, warn};

use super::{
    migrate::{self, Context},
    resp::Value,
};
use crate::routing::HASH_SLOT_COUNT;

/// The most slots one move of a rebalance takes: the range is frozen on the source from the
/// end of its copy until the map reaches it, so smaller moves keep each freeze short.
const STEP: usize = 256;
/// How often the rebalance looks whether its move ended.
const POLL: Duration = Duration::from_millis(20);

/// One move of a plan: `first..=last` from shard `from_shard` of group `from` (indexes into
/// the configured groups) to shard `to_shard` of group `to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Step {
    pub first: u16,
    pub last: u16,
    pub from: usize,
    pub from_shard: usize,
    pub to: usize,
    pub to_shard: usize,
}

/// How the rebalance this leader runs (or ran last) is going.
#[derive(Clone, Debug, Default)]
pub struct Progress {
    /// `running`, `done`, `cancelled`, `failed` or `stopped`.
    pub state: &'static str,
    pub moves: usize,
    pub left: usize,
    pub error: Option<String>,
    /// `COORDINATOR CANCEL` asked for the rebalance to end after its move.
    pub stop: bool,
}

impl Progress {
    pub fn running(&self) -> bool {
        self.state == "running"
    }

    /// The fields `COORDINATOR STATUS` adds.
    pub fn pairs(&self) -> Vec<(String, String)> {
        let mut pairs = vec![
            ("rebalance".into(), self.state.into()),
            ("rebalance_moves".into(), self.moves.to_string()),
            ("rebalance_left".into(), self.left.to_string()),
        ];

        if let Some(error) = &self.error {
            pairs.push(("rebalance_error".into(), error.clone()));
        }
        pairs
    }
}

/// The moves that even out `owners` (every slot's group and shard) across groups with
/// `shards[group]` shards each: every group ends with its share of the slots, within one.
/// Each move takes up to [`STEP`] slots from the group with the most to spare to the one
/// that lacks the most, from its fullest shard to the other's emptiest.
pub fn plan(shards: &[usize], owners: &[(usize, usize)]) -> Vec<Step> {
    let total: usize = shards.iter().sum();

    if total == 0 || owners.len() != HASH_SLOT_COUNT {
        return Vec::new();
    }
    let mut owners = owners.to_vec();
    let mut counts: Vec<Vec<usize>> = shards.iter().map(|&count| vec![0; count]).collect();

    for &(group, shard) in &owners {
        if let Some(count) = counts
            .get_mut(group)
            .and_then(|shards| shards.get_mut(shard))
        {
            *count += 1;
        }
    }
    let held = |counts: &[Vec<usize>], group: usize| counts[group].iter().sum::<usize>();
    let mut targets: Vec<usize> = shards
        .iter()
        .map(|&count| HASH_SLOT_COUNT * count / total)
        .collect();
    // The slots the shares leave over go to the groups that hold the most already, so the
    // fewest move.
    let mut fullest: Vec<usize> = (0..shards.len()).filter(|&g| shards[g] > 0).collect();

    fullest.sort_by_key(|&group| std::cmp::Reverse(held(&counts, group)));
    for &group in fullest
        .iter()
        .cycle()
        .take(HASH_SLOT_COUNT - targets.iter().sum::<usize>())
    {
        targets[group] += 1;
    }
    let mut steps = Vec::new();

    loop {
        let spare = |group: usize| held(&counts, group).saturating_sub(targets[group]);
        let lack = |group: usize| targets[group].saturating_sub(held(&counts, group));
        let (Some(from), Some(to)) = (
            (0..shards.len())
                .filter(|&g| spare(g) > 0)
                .max_by_key(|&g| (spare(g), std::cmp::Reverse(g))),
            (0..shards.len())
                .filter(|&g| lack(g) > 0)
                .max_by_key(|&g| (lack(g), std::cmp::Reverse(g))),
        ) else {
            return steps;
        };
        let from_shard = (0..shards[from])
            .max_by_key(|&s| (counts[from][s], std::cmp::Reverse(s)))
            .unwrap_or(0);
        let to_shard = (0..shards[to]).min_by_key(|&s| counts[to][s]).unwrap_or(0);
        let wanted = STEP.min(spare(from)).min(lack(to));
        let (first, last) = pick(&owners, (from, from_shard), (to, to_shard), wanted);

        for owner in &mut owners[first..=last] {
            *owner = (to, to_shard);
        }
        let moved = last - first + 1;

        counts[from][from_shard] -= moved;
        counts[to][to_shard] += moved;
        steps.push(Step {
            first: first as u16,
            last: last as u16,
            from,
            from_shard,
            to,
            to_shard,
        });
    }
}

/// Up to `wanted` slots of one run of `from`'s slots: next to a slot of `to` when a run
/// touches one, so the map keeps one range where it would get two, else the end of the
/// longest run.
fn pick(
    owners: &[(usize, usize)],
    from: (usize, usize),
    to: (usize, usize),
    wanted: usize,
) -> (usize, usize) {
    let mut runs: Vec<(usize, usize)> = Vec::new();

    for (slot, &owner) in owners.iter().enumerate() {
        if owner != from {
            continue;
        }
        match runs.last_mut() {
            Some((_, last)) if *last + 1 == slot => *last = slot,
            _ => runs.push((slot, slot)),
        }
    }
    for &(first, last) in &runs {
        let take = wanted.min(last - first + 1);

        if owners.get(last + 1) == Some(&to) {
            return (last + 1 - take, last);
        }
        if first > 0 && owners[first - 1] == to {
            return (first, first + take - 1);
        }
    }
    // A group with slots to spare has a run.
    let (first, last) = runs
        .iter()
        .copied()
        .max_by_key(|&(first, last)| last - first)
        .unwrap_or((0, 0));
    let take = wanted.min(last - first + 1);

    (last + 1 - take, last)
}

/// `COORDINATOR REBALANCE [DRYRUN]`: the plan's moves as text with `DRYRUN`; otherwise
/// starts running it and answers how many moves it holds now.
pub async fn start(context: &Context, dry: bool) -> Result<Value, String> {
    if !context.leading().await {
        return Err(context.not_leader().await);
    }
    let config = &context.config;
    let shards: Option<Vec<usize>> = config.groups.iter().map(|group| group.shards).collect();
    let shards = shards.ok_or("ERR rebalancing needs a multi-node cluster (groups with shards)")?;
    let moves = context.machine.moves();
    let steps = plan(&shards, &config.owners(&moves.moved));

    if dry {
        return Ok(Value::array(
            steps
                .iter()
                .map(|step| {
                    Value::bulk(format!(
                        "{}-{} {}/{} -> {}/{}",
                        step.first,
                        step.last,
                        config.groups[step.from].name,
                        step.from_shard,
                        config.groups[step.to].name,
                        step.to_shard
                    ))
                })
                .collect(),
        ));
    }
    {
        let mut mover = context.mover.lock();

        if mover.rebalance.as_ref().is_some_and(Progress::running) {
            return Err("ERR a rebalance is in progress".into());
        }
        if moves.current.is_some() {
            return Err("ERR another move is in progress".into());
        }
        mover.rebalance = Some(Progress {
            state: if steps.is_empty() { "done" } else { "running" },
            left: steps.len(),
            ..Progress::default()
        });
    }
    if !steps.is_empty() {
        info!(moves = steps.len(), "rebalancing the slots");
        let context = context.clone();

        tokio::spawn(async move { drive(&context).await });
    }
    Ok(Value::Integer(steps.len() as i64))
}

/// Runs the plan's first move until none is left, working the plan out again after each.
async fn drive(context: &Context) {
    let config = &context.config;
    let shards: Vec<usize> = config
        .groups
        .iter()
        .map(|group| group.shards.unwrap_or(0))
        .collect();
    let mut moves = 0;
    let (state, error) = 'steps: loop {
        let steps = plan(&shards, &config.owners(&context.machine.moves().moved));
        {
            let mut mover = context.mover.lock();

            if let Some(progress) = &mut mover.rebalance {
                progress.left = steps.len();
                progress.moves = moves;
                if progress.stop {
                    break 'steps ("cancelled", None);
                }
            }
        }
        let Some(step) = steps.first() else {
            break ("done", None);
        };
        let to = &config.groups[step.to].name;
        let id = match migrate::start(context, step.first, step.last, to, step.to_shard).await {
            Ok(id) => id,
            Err(error) => break ("failed", Some(error)),
        };

        // Ended once the move left the Raft state and its driver reported how.
        loop {
            tokio::time::sleep(POLL).await;
            if context.machine.moves().current.is_none() && context.mover.lock().driving.is_none() {
                break;
            }
            if !context.leading().await {
                break 'steps (
                    "stopped",
                    Some(
                        "this coordinator stopped leading; run COORDINATOR REBALANCE on the leader"
                            .into(),
                    ),
                );
            }
        }
        let last = context.mover.lock().last.clone();
        let field = |name: &str| {
            last.iter()
                .find(|(known, _)| known == name)
                .map(|(_, value)| value.as_str())
        };

        if !(field("state") == Some("done")
            && field("id").is_some_and(|known| known.starts_with(&id)))
        {
            if context
                .mover
                .lock()
                .rebalance
                .as_ref()
                .is_some_and(|p| p.stop)
            {
                break ("cancelled", None);
            }
            break (
                "failed",
                Some(
                    field("error")
                        .unwrap_or("the move did not finish")
                        .to_owned(),
                ),
            );
        }
        moves += 1;
    };

    match &error {
        Some(error) => warn!(moves, state, %error, "the rebalance ended"),
        None => info!(moves, state, "the rebalance ended"),
    }
    let mut mover = context.mover.lock();

    if let Some(progress) = &mut mover.rebalance {
        progress.state = state;
        progress.moves = moves;
        progress.error = error;
        if state == "done" {
            progress.left = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Owners where group `g` holds `ranges[g]` on shard 0.
    fn owners(ranges: &[&[(usize, usize)]]) -> Vec<(usize, usize)> {
        let mut owners = vec![(usize::MAX, 0); HASH_SLOT_COUNT];

        for (group, ranges) in ranges.iter().enumerate() {
            for &(first, last) in *ranges {
                for owner in &mut owners[first..=last] {
                    *owner = (group, 0);
                }
            }
        }
        owners
    }

    /// The owners after `steps`, checking each moves one run of one shard.
    fn apply(mut owners: Vec<(usize, usize)>, steps: &[Step]) -> Vec<(usize, usize)> {
        for step in steps {
            assert!(step.last - step.first < STEP as u16, "{step:?}");
            assert_ne!(step.from, step.to);
            for owner in &mut owners[usize::from(step.first)..=usize::from(step.last)] {
                assert_eq!(*owner, (step.from, step.from_shard), "{step:?}");
                *owner = (step.to, step.to_shard);
            }
        }
        owners
    }

    fn held(owners: &[(usize, usize)], group: usize) -> usize {
        owners.iter().filter(|owner| owner.0 == group).count()
    }

    #[test]
    fn balanced_slots_need_no_move() {
        let even = owners(&[&[(0, 8191)], &[(8192, 16383)]]);

        assert!(plan(&[1, 1], &even).is_empty());
        // One slot over is as even as an odd split gets.
        let three = owners(&[&[(0, 5461)], &[(5462, 10922)], &[(10923, 16383)]]);

        assert!(plan(&[1, 1, 1], &three).is_empty());
    }

    #[test]
    fn an_empty_group_gets_its_share_in_moves_of_at_most_256_slots() {
        let start = owners(&[&[(0, 16383)], &[]]);
        let steps = plan(&[1, 1], &start);
        let end = apply(start, &steps);

        assert_eq!(steps.len(), 8192 / STEP);
        assert_eq!((held(&end, 0), held(&end, 1)), (8192, 8192));
        // Every move takes the end of what group 0 keeps, next to what group 1 got: the
        // map stays two ranges.
        assert!(end[..8192].iter().all(|owner| owner.0 == 0));
    }

    #[test]
    fn shares_follow_the_shard_counts() {
        let start = owners(&[&[(0, 9999)], &[(10000, 16383)], &[]]);
        let steps = plan(&[2, 1, 1], &start);
        let end = apply(start, &steps);

        assert_eq!(
            (held(&end, 0), held(&end, 1), held(&end, 2)),
            (8192, 4096, 4096)
        );
        assert!(plan(&[2, 1, 1], &end).is_empty());
    }

    #[test]
    fn moves_go_to_the_emptiest_shard_of_the_group_lacking_slots() {
        let start = owners(&[&[(0, 16383)], &[]]);
        let steps = plan(&[1, 2], &start);
        let end = apply(start, &steps);
        let on = |shard| end.iter().filter(|&&owner| owner == (1, shard)).count();

        assert_eq!(held(&end, 1), 10922);
        assert!(on(0).abs_diff(on(1)) <= STEP, "{} {}", on(0), on(1));
    }

    #[test]
    fn scattered_slots_are_moved_a_run_at_a_time() {
        // Group 0 holds every other 100 slots; no run is longer than one move.
        let mut start = vec![(1, 0); HASH_SLOT_COUNT];

        for (slot, owner) in start.iter_mut().enumerate() {
            if (slot / 100) % 2 == 0 || slot < 6000 {
                *owner = (0, 0);
            }
        }
        let steps = plan(&[1, 1], &start);
        let end = apply(start, &steps);

        assert_eq!((held(&end, 0), held(&end, 1)), (8192, 8192));
    }
}
