use std::vec;

use crate::models::racecard::{Horse, PastPerformance};
use crate::models::trip_handicapping::{TripResult, TripScore};

use chrono::{NaiveDate};
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

fn parse_race_date(value: &str) -> Option<NaiveDate> {
    let v = value.trim();
    if v.len() == 8 && v.chars().all(|c| c.is_ascii_digit()) {
        NaiveDate::parse_from_str(v, "%Y%m%d").ok()
    } else {
        NaiveDate::parse_from_str(v, "%m/%d/%Y").ok()
    }
}

fn finish_position_number(pp: &PastPerformance) -> Option<u32> {
    let raw = pp.finish_position.trim();
    if raw.is_empty() {
        return None;
    }

    let digits: String = raw.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }

    digits.parse::<u32>().ok()
}

fn finish_position_label(pos: u32) -> Option<&'static str> {
    match pos {
        1 => Some("winner"),
        2 => Some("2nd"),
        3 => Some("3rd"),
        _ => None,
    }
}

fn bad_trip_reason(text: &str) -> Option<&'static str> {
    let t = text.to_lowercase();
    let bad_phrases = [
        "no factor",
        "no threat",
        "never involved",
        "was never a threat",
        "little impact",
        "faltered 2nd turn",
        "weakened 2nd turn",
        "stopped",
        "gave way",
        "done early",
        "eased",
        "empty",
        "folded",
        "trailed 1/8",
        "tired",
        "failed to rally",
        "failed to respond",
        "faltered",
        "mildly",
        "no rally",
        "finished early",
        "remained back",
        "trailed throughout",
        "trailed in",
        "backed up stretch",
        "lacked closing kick",
        "lacked kick",
        "fade before a half",
        "little before drive",
        "little for drive",
    ];
    bad_phrases.into_iter().find(|p| t.contains(p))
}

fn map_good_reason(reason: &str) -> String {
    if let Some(stripped) = reason.strip_prefix("wide_") {
        if let Some(w) = stripped.strip_suffix('w') {
            return format!("wide {w}w");
        }
    }

    match reason {
        "start_trouble" => "bad start".to_string(),
        "start_contact" => "squeezed/crowded start".to_string(),
        "traffic/interference" => "traffic/interference".to_string(),
        "bump/brush" => "bumped/roughed".to_string(),
        "used/pace_pressure" => "pace pressure".to_string(),
        "close_finish_bonus" => "close finish".to_string(),
        "mid_finish_bonus" => "mid finish".to_string(),
        "fast_early_context" => "fast early pace".to_string(),
        "stopped_clip" => "stopped".to_string(),
        "big_defeat_clip" => "big defeat".to_string(),
        "noncompetitive_phrase" => "noncompetitive".to_string(),
        "dnf_like" => "pulled up/unseated".to_string(),
        _ => reason.replace('_', " "),
    }
}

fn good_trip_reason(ts: &TripScore) -> String {
    if !ts.why.is_empty() {
        map_good_reason(&ts.why[0])
    } else if !ts.headline.is_empty() && ts.headline != "trip" {
        if let Some(w) = ts.headline.strip_suffix('w') {
            format!("wide {w}w")
        } else {
            ts.headline.clone()
        }
    } else {
        "trouble trip".to_string()
    }
}

fn classify_trip(pp: &PastPerformance) -> (String, i32) {
    let text = merged_trip_text(pp);
    let trimmed = text.trim();

    if let Some(pos) = finish_position_number(pp) {
        if pos <= 3 {
            let label = finish_position_label(pos).unwrap_or("top-3 finish");
            return (format!("Good: {label}"), 1);
        }
    }

    if trimmed.is_empty() {
        return ("Excusable: no trip note".to_string(), 0);
    }

    if contains_any(trimmed, &["flattened late"]) {
        return ("Excusable: flattened late".to_string(), 0);
    }
    if contains_any(trimmed, &["faltered drive"]) {
        return ("Excusable: faltered drive".to_string(), 0);
    }
    if contains_any(trimmed, &["passed tiring foes"]) {
        return ("Excusable: passed tiring foes".to_string(), 0);
    }
    if contains_any(trimmed, &["passing tiring rivals"]) {
        return ("Excusable: passing tiring rivals".to_string(), 0);
    }

    if let Some(reason) = bad_trip_reason(trimmed) {
        return (format!("Bad: {}", reason), -1);
    }

    if contains_any(trimmed, &["clearly 2nd best", "driving"]) {
        let t = trimmed.to_lowercase();
        let reason = if t.contains("clearly 2nd best") {
            "clearly 2nd best"
        } else {
            "driving"
        };
        return (format!("Good: {}", reason), 1);
    }

    let raw = raw_trip_score(trimmed);
    let scored = apply_context_adjust(raw, pp);

    let good_trip = scored.adj >= 1.0
        || scored.raw >= 0.9
        || max_path_from_text(trimmed).unwrap_or(0) >= 3;

    if good_trip {
        (format!("Good: {}", good_trip_reason(&scored)), 1)
    } else {
        ("Excusable: routine trip".to_string(), 0)
    }
}

pub fn trip_data_for_horse(horse: &Horse, race_date: &String) -> Option<TripResult> {
    let race_day = parse_race_date(race_date)?;

    let mut picked: Vec<((String, i32), i64)> = Vec::with_capacity(3);

    for pp in horse.past_performances.iter() {
        let pp_day = match parse_race_date(&pp.race_date) {
            Some(d) => d,
            None => continue,
        };

        if pp_day > race_day {
            continue;
        }

        let time_since_last_pp = race_day.signed_duration_since(pp_day);

        picked.push((classify_trip(pp), time_since_last_pp.num_days()));

        if picked.len() == 3 {
            break;
        }
    }

    if picked.is_empty() {
        return None;
    }

    let weights = [50, 30, 20];
    let mut score: i32 = 0;
    for (i, ((_, grade), _)) in picked.iter().enumerate() {
        let w = weights.get(i).copied().unwrap_or(0);
        score += w * (*grade);
    }

    let mut comments = vec![
        "".to_string(),
        "".to_string(),
        "".to_string(),
    ];

    let mut days_back =vec![
        0,
        0,
        0,
    ];

    for (i, (comment, days)) in picked.into_iter().enumerate().take(3) {
        comments[i] = comment.0.replace(',', " ");
        days_back[i] = days;
    }

    Some(TripResult {
        score,
        days_back_1st: days_back[0],
        comment_1st: comments[0].clone(),
        days_back_2nd: days_back[1],
        comment_2nd: comments[1].clone(),
        days_back_3rd: days_back[2],
        comment_3rd: comments[2].clone(),
    })
}
