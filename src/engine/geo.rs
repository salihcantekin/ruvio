use super::*;

pub(super) fn geo_add<'a, C: Clock, I: StorageIndex, S: MutationSink, W: ListWaiterSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    waiters: &mut W,
    key: crate::protocol::Bytes<'a>,
    members: Vec<(f64, f64, crate::protocol::Bytes<'a>)>,
    options: crate::protocol::SortedSetAddOptions,
) -> Result<Response<'a>, S::Error> {
    let mut pairs = Vec::with_capacity(members.len());

    for (longitude, latitude, member) in members {
        let Some(score) = crate::geo::encode_score(longitude, latitude) else {
            return Ok(Response::OwnedError(
                format!("ERR invalid longitude,latitude pair {longitude},{latitude}").into_bytes(),
            ));
        };

        pairs.push((score, member));
    }

    sorted_set_add(shard, sink, waiters, key, pairs.into(), options)
}

pub(super) fn geo_dist<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: crate::protocol::Bytes<'a>,
    from: crate::protocol::Bytes<'a>,
    to: crate::protocol::Bytes<'a>,
    unit: f64,
) -> Response<'a> {
    let left = match shard.sorted_set_score(&key, &from) {
        Ok(score) => score,
        Err(_) => return wrong_type(),
    };

    let right = match shard.sorted_set_score(&key, &to) {
        Ok(score) => score,
        Err(_) => return wrong_type(),
    };

    let (Some(left), Some(right)) = (left, right) else {
        return Response::Bulk(None);
    };

    let Some((left_lon, left_lat)) = crate::geo::decode_score(left) else {
        return Response::Bulk(None);
    };

    let Some((right_lon, right_lat)) = crate::geo::decode_score(right) else {
        return Response::Bulk(None);
    };

    Response::Double(crate::geo::distance_m(left_lon, left_lat, right_lon, right_lat) / unit)
}

pub(super) fn geo_pos<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: crate::protocol::Bytes<'a>,
    members: Vec<crate::protocol::Bytes<'a>>,
) -> Response<'a> {
    let mut rows = Vec::with_capacity(members.len());

    for member in members {
        match shard.sorted_set_score(&key, &member) {
            Err(_) => return wrong_type(),
            Ok(None) => rows.push(Response::NullArray),
            Ok(Some(score)) => {
                let Some((longitude, latitude)) = crate::geo::decode_score(score) else {
                    rows.push(Response::NullArray);

                    continue;
                };

                rows.push(Response::Exec(vec![
                    Response::Double(longitude),
                    Response::Double(latitude),
                ]));
            }
        }
    }

    Response::Exec(rows)
}

pub(super) fn geo_hash<'a, C: Clock, I: StorageIndex>(
    shard: &'a KvShard<C, I>,
    key: crate::protocol::Bytes<'a>,
    members: Vec<crate::protocol::Bytes<'a>>,
) -> Response<'a> {
    let mut rows = Vec::with_capacity(members.len());

    for member in members {
        match shard.sorted_set_score(&key, &member) {
            Err(_) => return wrong_type(),
            Ok(None) => rows.push(Response::Bulk(None)),
            Ok(Some(score)) => {
                let Some((longitude, latitude)) = crate::geo::decode_score(score) else {
                    rows.push(Response::Bulk(None));

                    continue;
                };

                let Some(text) = crate::geo::standard_geohash(longitude, latitude) else {
                    rows.push(Response::Bulk(None));

                    continue;
                };

                rows.push(Response::Bulk(Some(Cow::Owned(text.to_vec()))));
            }
        }
    }

    Response::Exec(rows)
}

pub(super) struct GeoHit {
    pub(super) member: Vec<u8>,
    pub(super) score: f64,
    pub(super) place: crate::geo::Place,
}

pub(super) struct NearestHit {
    pub(super) hit: GeoHit,
}

impl PartialEq for NearestHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for NearestHit {}

impl PartialOrd for NearestHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NearestHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.hit
            .place
            .distance_m
            .total_cmp(&other.hit.place.distance_m)
    }
}

pub(super) fn keep_nearest(
    nearest: &mut BinaryHeap<NearestHit>,
    limit: usize,
    member: &[u8],
    score: f64,
    place: crate::geo::Place,
) {
    if nearest.len() == limit
        && nearest
            .peek()
            .is_some_and(|worst| place.distance_m >= worst.hit.place.distance_m)
    {
        return;
    }

    nearest.push(NearestHit {
        hit: GeoHit {
            member: member.to_vec(),
            score,
            place,
        },
    });

    if nearest.len() > limit {
        nearest.pop();
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn geo_search<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    key: crate::protocol::Bytes<'a>,
    center: crate::protocol::GeoCenter<'a>,
    area: crate::protocol::GeoArea,
    unit: f64,
    descending: bool,
    ordered: bool,
    count: Option<usize>,
    any: bool,
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
    store: Option<crate::protocol::GeoStore<'a>>,
) -> Result<Response<'a>, S::Error> {
    if shard.contains_key(&key) && !accepts_type(shard, &key, ValueKind::SortedSet) {
        return Ok(wrong_type());
    }

    if !shard.contains_key(&key) {
        if let Some(store) = store {
            return geo_store_hits(shard, sink, store, Vec::new(), unit);
        }

        return Ok(Response::Exec(Vec::new()));
    }

    let (center_lon, center_lat) = match center {
        crate::protocol::GeoCenter::Coordinates(longitude, latitude) => {
            if crate::geo::encode_score(longitude, latitude).is_none() {
                return Ok(Response::OwnedError(
                    format!("ERR invalid longitude,latitude pair {longitude},{latitude}")
                        .into_bytes(),
                ));
            }

            (longitude, latitude)
        }

        crate::protocol::GeoCenter::Member(member) => {
            let score = shard
                .sorted_set_score(&key, &member)
                .expect("sorted-set type was validated");
            let Some(score) = score else {
                return Ok(Response::Error(
                    b"ERR could not decode requested zset member",
                ));
            };

            let Some(point) = crate::geo::decode_score(score) else {
                return Ok(Response::Error(
                    b"ERR could not decode requested zset member",
                ));
            };

            point
        }
    };

    let shape = match area {
        crate::protocol::GeoArea::Radius(radius_m) => crate::geo::Shape::Circle { radius_m },
        crate::protocol::GeoArea::Box { width_m, height_m } => {
            crate::geo::Shape::Box { width_m, height_m }
        }
    };

    let boxes = crate::geo::search_boxes(center_lon, center_lat, shape);

    if let Some(limit) = count.filter(|_| ordered && !any) {
        let mut nearest = BinaryHeap::new();

        for hash in boxes {
            let (min, max) = crate::geo::score_window(hash);

            shard
                .sorted_set_visit_scores(
                    &key,
                    crate::protocol::ScoreBound::Inclusive(min),
                    crate::protocol::ScoreBound::Exclusive(max),
                    |member, score| {
                        let Some(place) =
                            crate::geo::place_if_inside(shape, center_lon, center_lat, score)
                        else {
                            return true;
                        };

                        keep_nearest(&mut nearest, limit, member, score, place);
                        true
                    },
                )
                .expect("sorted-set type was validated");
        }

        let mut hits: Vec<GeoHit> = nearest
            .into_sorted_vec()
            .into_iter()
            .map(|hit| hit.hit)
            .collect();

        if descending {
            hits.reverse();
        }

        if let Some(store) = store {
            return geo_store_hits(shard, sink, store, hits, unit);
        }

        return Ok(geo_search_reply(
            hits, with_dist, with_hash, with_coord, unit,
        ));
    }

    let mut hits = Vec::new();

    for hash in boxes {
        let (min, max) = crate::geo::score_window(hash);
        let entries = shard
            .sorted_set_range_by_score(
                &key,
                crate::protocol::ScoreBound::Inclusive(min),
                crate::protocol::ScoreBound::Exclusive(max),
            )
            .expect("sorted-set type was validated");

        for (member, score) in entries {
            let Some(place) = crate::geo::place_if_inside(shape, center_lon, center_lat, score)
            else {
                continue;
            };

            hits.push(GeoHit {
                member: member.to_vec(),
                score,
                place,
            });

            if any && count.is_some_and(|limit| hits.len() >= limit) {
                break;
            }
        }

        if any && count.is_some_and(|limit| hits.len() >= limit) {
            break;
        }
    }

    if ordered {
        hits.sort_by(|left, right| left.place.distance_m.total_cmp(&right.place.distance_m));

        if descending {
            hits.reverse();
        }
    }

    if let Some(limit) = count {
        hits.truncate(limit);
    }

    if let Some(store) = store {
        return geo_store_hits(shard, sink, store, hits, unit);
    }

    Ok(geo_search_reply(
        hits, with_dist, with_hash, with_coord, unit,
    ))
}

pub(super) fn geo_store_hits<'a, C: Clock, I: StorageIndex, S: MutationSink>(
    shard: &'a mut KvShard<C, I>,
    sink: &mut S,
    store: crate::protocol::GeoStore<'a>,
    hits: Vec<GeoHit>,
    unit: f64,
) -> Result<Response<'a>, S::Error> {
    // Admitted once for the whole result before the old destination goes, as Z*STORE does:
    // a refusal must leave the destination as it was.
    let extra = hits.iter().map(|hit| hit.member.len() + 16).sum::<usize>()
        + store.destination.len()
        + ADMISSION_OVERHEAD_BYTES;

    if let Some(rejected) = before_mutation(shard, sink, [store.destination.as_ref()], extra)? {
        return Ok(rejected);
    }

    if !hits.is_empty()
        && !shard.sorted_set_make_room(
            &store.destination,
            true,
            hits.iter().map(|hit| hit.member.as_slice()),
        )
    {
        return Ok(sorted_set_error(SortedSetError::Full));
    }

    if shard.contains_key(&store.destination) {
        let keys = [store.destination.clone()];

        sink.record(Mutation::Delete { keys: &keys })?;

        shard.delete(&store.destination);
    }

    if hits.is_empty() {
        return Ok(Response::Integer(0));
    }

    let pairs: Vec<(f64, crate::protocol::Bytes<'_>)> = hits
        .iter()
        .map(|hit| {
            let score = if store.distance {
                hit.place.distance_m / unit
            } else {
                hit.score
            };

            (score, std::borrow::Cow::Borrowed(hit.member.as_slice()))
        })
        .collect();

    // Admitted above, so the result is written as Z*STORE writes its own.
    sink.record(Mutation::SortedSetAdd {
        key: &store.destination,
        pairs: &pairs,
    })?;
    match shard.sorted_set_add_pairs(&store.destination, &pairs) {
        Ok(added) => Ok(Response::Integer(added as i64)),
        Err(error) => {
            debug_assert!(false, "geo store failed after validation");
            Ok(sorted_set_error(error))
        }
    }
}

pub(super) fn geo_search_reply<'a>(
    hits: Vec<GeoHit>,
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
    unit: f64,
) -> Response<'a> {
    let nested = with_dist || with_hash || with_coord;

    Response::Exec(
        hits.into_iter()
            .map(|hit| {
                if !nested {
                    return Response::Bulk(Some(Cow::Owned(hit.member)));
                }

                let mut row = vec![Response::Bulk(Some(Cow::Owned(hit.member)))];

                if with_dist {
                    row.push(Response::Double(hit.place.distance_m / unit));
                }

                if with_hash {
                    row.push(Response::Integer(hit.score as i64));
                }

                if with_coord {
                    row.push(Response::Exec(vec![
                        Response::Double(hit.place.longitude),
                        Response::Double(hit.place.latitude),
                    ]));
                }

                Response::Exec(row)
            })
            .collect(),
    )
}
