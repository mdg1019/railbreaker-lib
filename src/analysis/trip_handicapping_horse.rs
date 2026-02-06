use crate::models::racecard::{Horse, PastPerformance};
use crate::models::trip_handicapping::{TripScore, BetBackPick};

use regex::Regex;


fn merged_trip_text(pp: &PastPerformance) -> String {
    let mut parts: Vec<String> = vec![];

    let ext = pp.extended_start_comment.trim();
    if !ext.is_empty() {
        let re_date_prefix = Regex::new(r"^\s*\d{2}-\d{2}-\d{2}\s+").unwrap();
        parts.push(re_date_prefix.replace(ext, "").to_string());
    }

    let short = pp.trip_comment.trim();
    if !short.is_empty() {
        let short_norm = short.to_lowercase();
        let ext_norm = parts.join(" ").to_lowercase();
        if !ext_norm.contains(&short_norm) {
            parts.push(short.to_string());
        }
    }

    parts.join("; ")
}

fn max_path_from_text(text: &str) -> Option<u32> {
    let t = text.to_lowercase();

    let re_w = Regex::new(r"(?:(\d)\s*-\s*(\d)\s*w)|(?:(\d{1,2})\s*w)\b").unwrap();
    let mut best: Option<u32> = None;

    for cap in re_w.captures_iter(&t) {
        if let (Some(a), Some(b)) = (cap.get(1), cap.get(2)) {
            let aa: u32 = a.as_str().parse().ok()?;
            let bb: u32 = b.as_str().parse().ok()?;
            best = Some(best.unwrap_or(0).max(aa.max(bb)));
        } else if let Some(x) = cap.get(3) {
            if let Ok(v) = x.as_str().parse::<u32>() {
                best = Some(best.unwrap_or(0).max(v));
            }
        }
    }

    let re_p = Regex::new(r"\b(\d{1,2})\s*(?:p|pth|wd|wide)\b").unwrap();
    for cap in re_p.captures_iter(&t) {
        if let Ok(v) = cap[1].parse::<u32>() {
            best = Some(best.unwrap_or(0).max(v));
        }
    }

    best
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    let t = text.to_lowercase();
    needles.iter().any(|n| t.contains(n))
}

fn raw_trip_score(text: &str) -> TripScore {
    let mut raw = 0.0;
    let mut why: Vec<String> = vec![];

    if contains_any(
        text,
        &[
            "stumbled",
            "bobbled",
            "off slow",
            "slow start",
            "hesitated",
            "hit gate",
        ],
    ) {
        raw += 1.6;
        why.push("start_trouble".into());
    }
    if contains_any(
        text,
        &[
            "broke in",
            "broke out",
            "ducked",
            "pinched",
            "squeezed",
            "crowded",
        ],
    ) {
        raw += 1.3;
        why.push("start_contact".into());
    }

    if contains_any(
        text,
        &[
            "checked",
            "steady",
            "steadied",
            "blocked",
            "held up",
            "tight",
            "shuffled",
            "stym",
            "climbed",
            "altered course",
        ],
    ) {
        raw += 2.6;
        why.push("traffic/interference".into());
    }
    if contains_any(text, &["bumped", "brush", "clipped heels", "roughed"]) {
        raw += 1.4;
        why.push("bump/brush".into());
    }

    if let Some(p) = max_path_from_text(text) {
        if p >= 3 {
            let extra = (p as f64) - 2.0;
            let add = extra * 0.65;
            raw += add;
            why.push(format!("wide_{}w", p));
        }
    }

    if contains_any(
        text,
        &[
            "rushed",
            "used",
            "dueled",
            "pressed",
            "pulled strongly",
            "hard chase",
        ],
    ) {
        raw += 0.9;
        why.push("used/pace_pressure".into());
    }

    if contains_any(
        text,
        &[
            "no factor",
            "no threat",
            "never involved",
            "was never a threat",
        ],
    ) {
        raw -= 0.8;
        why.push("noncompetitive_phrase".into());
    }
    if contains_any(text, &["pulled up", "unseated"]) {
        raw += 0.0;
        why.push("dnf_like".into());
    }

    let headline = if let Some(p) = max_path_from_text(text) {
        if p >= 4 { format!("{p}w") } else { "".into() }
    } else {
        "".into()
    };

    let headline = if !headline.is_empty() {
        headline
    } else if contains_any(text, &["checked", "steady", "blocked", "tight", "shuffled"]) {
        "traffic".into()
    } else if contains_any(text, &["stumbled", "off slow", "hit gate"]) {
        "bad start".into()
    } else {
        "trip".into()
    };

    TripScore {
        raw: raw.max(0.0),
        adj: 0.0,
        headline,
        why,
    }
}

fn apply_context_adjust(mut ts: TripScore, pp: &PastPerformance) -> TripScore {
    let mut adj = ts.raw;

    let beaten = pp
        .finish_between_lengths
        .filter(|v| *v > 0.0)
        .or(pp.finish_between_lengths_leader.filter(|v| *v > 0.0))
        .unwrap_or(99.0);

    if beaten <= 3.0 {
        adj += 1.2;
        ts.why.push("close_finish_bonus".into());
    } else if beaten <= 6.0 {
        adj += 0.6;
        ts.why.push("mid_finish_bonus".into());
    } else if beaten >= 15.0 {
        adj -= 1.1;
        ts.why.push("big_defeat_clip".into());
    }

    let text = merged_trip_text(pp).to_lowercase();
    if contains_any(
        &text,
        &[
            "stopped",
            "jogged in",
            "was finished",
            "gave way",
            "done early",
        ],
    ) && beaten >= 10.0
    {
        adj -= 1.2;
        ts.why.push("stopped_clip".into());
    }

    if let (Some(p2), Some(lp)) = (pp.bris_2f_pace, pp.bris_late_pace) {
        if p2 >= 85 && lp > 0 && lp <= 70 {
            adj += 0.4;
            ts.why.push("fast_early_context".into());
        }
    }

    ts.adj = adj.max(0.0);
    ts
}

fn dist_to_furlongs(distance_yards: Option<i32>) -> f64 {
    match distance_yards {
        Some(distance_yards) if distance_yards > 0 => (distance_yards as f64) / 220.0,
        _ => 0.0,
    }
}

pub fn best_bet_back_line(horse: &Horse) -> Option<BetBackPick> {
    let mut best: Option<BetBackPick> = None;

    for (i, pp) in horse.past_performances.iter().enumerate() {
        let text = merged_trip_text(pp);
        if text.trim().is_empty() {
            continue;
        }

        let raw = raw_trip_score(&text);
        let scored = apply_context_adjust(raw, pp);

        let pick = BetBackPick {
            pp_index: i,
            score: scored.clone(),
            surface: pp.surface.clone(),
            dist_f: dist_to_furlongs(pp.distance),
            date: pp.race_date.clone(),
            track: pp.track_code.clone(),
            adj_points: scored.adj * 1.5,
        };

        let better = best
            .as_ref()
            .map(|b| pick.score.adj > b.score.adj)
            .unwrap_or(true);

        if better {
            best = Some(pick);
        }
    }

    best
}
