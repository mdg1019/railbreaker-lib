use std::collections::HashMap;
use sqlx::{Row, SqlitePool, sqlite::SqliteRow};
use crate::models::racecard::{
    Horse, KeyTrainerStat, PastPerformance, Race, Racecard, Workout,
};

const RACE_COLUMNS: usize = 43;
const HORSE_COLUMNS: usize = 145;
const PAST_PERFORMANCE_COLUMNS: usize = 101;

fn placeholders(count: usize) -> String {
    std::iter::repeat("?")
        .take(count)
        .collect::<Vec<_>>()
        .join(", ")
}

pub async fn create_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let statements = [
        r#"
        CREATE TABLE IF NOT EXISTS racecards (
            id INTEGER PRIMARY KEY,
            zip_file_name TEXT NOT NULL,
            track_code TEXT NOT NULL,
            track TEXT NOT NULL,
            date TEXT NOT NULL,
            long_date TEXT NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS races (
            id INTEGER PRIMARY KEY,
            racecard_id INTEGER NOT NULL,
            race_number INTEGER,
            distance INTEGER,
            surface TEXT NOT NULL,
            race_type TEXT NOT NULL,
            age_sex_restrictions TEXT NOT NULL,
            todays_race_classification TEXT NOT NULL,
            purse INTEGER,
            claiming_price INTEGER,
            track_record REAL,
            race_conditions TEXT NOT NULL,
            todays_lasix_list TEXT NOT NULL,
            todays_bute_list TEXT NOT NULL,
            todays_coupled_list TEXT NOT NULL,
            todays_mutuel_list TEXT NOT NULL,
            simulcast_host_track_code TEXT NOT NULL,
            simulcast_host_track_race_number INTEGER,
            all_weather_surface_flag TEXT NOT NULL,
            race_conditions_line1 TEXT NOT NULL,
            race_conditions_line2 TEXT NOT NULL,
            race_conditions_line3 TEXT NOT NULL,
            race_conditions_line4 TEXT NOT NULL,
            race_conditions_line5 TEXT NOT NULL,
            race_conditions_line6 TEXT NOT NULL,
            low_claiming_price INTEGER,
            statebred_flag TEXT NOT NULL,
            wager_type_line1 TEXT NOT NULL,
            wager_type_line2 TEXT NOT NULL,
            wager_type_line3 TEXT NOT NULL,
            wager_type_line4 TEXT NOT NULL,
            wager_type_line5 TEXT NOT NULL,
            wager_type_line6 TEXT NOT NULL,
            wager_type_line7 TEXT NOT NULL,
            wager_type_line8 TEXT NOT NULL,
            wager_type_line9 TEXT NOT NULL,
            two_f_bris_pace_par INTEGER,
            four_f_bris_pace_par INTEGER,
            six_f_bris_pace_par INTEGER,
            bris_speed_for_class INTEGER,
            bris_late_pace_par INTEGER,
            post_times TEXT NOT NULL,
            post_time_pacific_military TEXT NOT NULL,
            todays_equibase_abbreviated_race_conditions TEXT NOT NULL,
            FOREIGN KEY (racecard_id) REFERENCES racecards(id) ON DELETE CASCADE
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS horses (
            id INTEGER PRIMARY KEY,
            race_id INTEGER NOT NULL,
            scratched INTEGER NOT NULL,
            trip_handicapping_info TEXT NOT NULL,
            post_position INTEGER,
            entry TEXT NOT NULL,
            claiming_price_of_horse INTEGER,
            breed_type TEXT NOT NULL,
            todays_nasal_strip_change INTEGER,
            todays_trainer TEXT NOT NULL,
            trainer_starts INTEGER,
            trainer_wins INTEGER,
            trainer_places INTEGER,
            trainer_shows INTEGER,
            todays_jockey TEXT NOT NULL,
            apprentice_weight_allowance INTEGER,
            jockey_starts INTEGER,
            jockey_wins INTEGER,
            jockey_places INTEGER,
            jockey_shows INTEGER,
            todays_owner TEXT NOT NULL,
            owners_silks TEXT NOT NULL,
            main_track_only_ae_indicator TEXT NOT NULL,
            program_number TEXT NOT NULL,
            morning_line_odds REAL,
            horse_name TEXT NOT NULL,
            year_of_birth INTEGER,
            horses_foaling_month INTEGER,
            sex TEXT NOT NULL,
            horses_color TEXT NOT NULL,
            weight INTEGER,
            sire TEXT NOT NULL,
            sires_sire TEXT NOT NULL,
            dam TEXT NOT NULL,
            dams_sire TEXT NOT NULL,
            breeder TEXT NOT NULL,
            state_country_where_bred TEXT NOT NULL,
            program_post_position TEXT NOT NULL,
            todays_medication_new INTEGER,
            todays_medication_old INTEGER,
            equipment_change INTEGER,
            lifetime_record_todays_distance_starts INTEGER,
            lifetime_record_todays_distance_wins INTEGER,
            lifetime_record_todays_distance_places INTEGER,
            lifetime_record_todays_distance_shows INTEGER,
            lifetime_record_todays_distance_earnings INTEGER,
            lifetime_record_todays_track_starts INTEGER,
            lifetime_record_todays_track_wins INTEGER,
            lifetime_record_todays_track_places INTEGER,
            lifetime_record_todays_track_shows INTEGER,
            lifetime_record_todays_track_earnings INTEGER,
            lifetime_record_turf_starts INTEGER,
            lifetime_record_turf_wins INTEGER,
            lifetime_record_turf_places INTEGER,
            lifetime_record_turf_shows INTEGER,
            lifetime_record_turf_earnings INTEGER,
            lifetime_record_wet_starts INTEGER,
            lifetime_record_wet_wins INTEGER,
            lifetime_record_wet_places INTEGER,
            lifetime_record_wet_shows INTEGER,
            lifetime_record_wet_earnings INTEGER,
            current_year_record_year INTEGER,
            current_year_record_starts INTEGER,
            current_year_record_wins INTEGER,
            current_year_record_places INTEGER,
            current_year_record_shows INTEGER,
            current_year_record_earnings INTEGER,
            previous_year_record_year INTEGER,
            previous_year_record_starts INTEGER,
            previous_year_record_wins INTEGER,
            previous_year_record_places INTEGER,
            previous_year_record_shows INTEGER,
            previous_year_record_earnings INTEGER,
            lifetime_record_starts INTEGER,
            lifetime_record_wins INTEGER,
            lifetime_record_places INTEGER,
            lifetime_record_shows INTEGER,
            lifetime_record_earnings INTEGER,
            bris_run_style TEXT NOT NULL,
            quirin_speed_points INTEGER,
            trainer_jockey_combo_starts INTEGER,
            trainer_jockey_combo_wins INTEGER,
            trainer_jockey_combo_places INTEGER,
            trainer_jockey_combo_shows INTEGER,
            trainer_jockey_combo_roi REAL,
            days_since_last_race INTEGER,
            lifetime_all_weather_starts INTEGER,
            lifetime_all_weather_wins INTEGER,
            lifetime_all_weather_places INTEGER,
            lifetime_all_weather_shows INTEGER,
            lifetime_all_weather_earnings INTEGER,
            best_bris_speed_all_weather_surface INTEGER,
            bris_prime_power_rating REAL,
            trainer_starts_current_year INTEGER,
            trainer_wins_current_year INTEGER,
            trainer_places_current_year INTEGER,
            trainer_shows_current_year INTEGER,
            trainer_roi_current_year REAL,
            trainer_starts_previous_year INTEGER,
            trainer_wins_previous_year INTEGER,
            trainer_places_previous_year INTEGER,
            trainer_shows_previous_year INTEGER,
            trainer_roi_previous_year REAL,
            jockey_starts_current_year INTEGER,
            jockey_wins_current_year INTEGER,
            jockey_places_current_year INTEGER,
            jockey_shows_current_year INTEGER,
            jockey_roi_current_year REAL,
            jockey_starts_previous_year INTEGER,
            jockey_wins_previous_year INTEGER,
            jockey_places_previous_year INTEGER,
            jockey_shows_previous_year INTEGER,
            jockey_roi_previous_year REAL,
            sire_stud_fee INTEGER,
            best_bris_speed_fast_track INTEGER,
            best_bris_speed_turf INTEGER,
            best_bris_speed_off_track INTEGER,
            best_bris_speed_distance INTEGER,
            auction_price INTEGER,
            where_when_sold_at_auction TEXT NOT NULL,
            bris_dirt_pedigree_rating TEXT NOT NULL,
            bris_mud_pedigree_rating TEXT NOT NULL,
            bris_turf_pedigree_rating TEXT NOT NULL,
            bris_distance_pedigree_rating TEXT NOT NULL,
            best_bris_speed_life INTEGER,
            best_bris_speed_most_recent_year INTEGER,
            best_bris_speed_2nd_most_recent_year INTEGER,
            best_bris_speed_todays_track INTEGER,
            starts_fast_dirt INTEGER,
            wins_fast_dirt INTEGER,
            places_fast_dirt INTEGER,
            shows_fast_dirt INTEGER,
            earnings_fast_dirt INTEGER,
            jockey_distance_turf_label TEXT NOT NULL,
            jockey_distance_turf_starts INTEGER,
            jockey_distance_turf_wins INTEGER,
            jockey_distance_turf_places INTEGER,
            jockey_distance_turf_shows INTEGER,
            jockey_distance_turf_roi REAL,
            jockey_distance_turf_earnings INTEGER,
            trainer_jockey_combo_starts_meet INTEGER,
            trainer_jockey_combo_wins_meet INTEGER,
            trainer_jockey_combo_places_meet INTEGER,
            trainer_jockey_combo_shows_meet INTEGER,
            trainer_jockey_combo_roi_meet REAL,
            note TEXT NOT NULL,
            FOREIGN KEY (race_id) REFERENCES races(id) ON DELETE CASCADE
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS workouts (
            id INTEGER PRIMARY KEY,
            horse_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            time REAL,
            track TEXT NOT NULL,
            distance INTEGER,
            condition TEXT NOT NULL,
            description TEXT NOT NULL,
            main_inner_track_indicator TEXT NOT NULL,
            workouts_that_day_distance INTEGER,
            rank INTEGER,
            FOREIGN KEY (horse_id) REFERENCES horses(id) ON DELETE CASCADE
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS past_performances (
            id INTEGER PRIMARY KEY,
            horse_id INTEGER NOT NULL,
            race_date TEXT NOT NULL,
            days_since_last_race INTEGER,
            track_code TEXT NOT NULL,
            bris_track_code TEXT NOT NULL,
            race_number INTEGER,
            track_condition TEXT NOT NULL,
            distance INTEGER,
            surface TEXT NOT NULL,
            special_chute_indicator TEXT NOT NULL,
            entrants INTEGER,
            post_position INTEGER,
            equipment TEXT NOT NULL,
            racename TEXT NOT NULL,
            medication INTEGER,
            trip_comment TEXT NOT NULL,
            winners_name TEXT NOT NULL,
            place_name TEXT NOT NULL,
            show_name TEXT NOT NULL,
            winners_weight INTEGER,
            place_weight INTEGER,
            show_weight INTEGER,
            winners_margin REAL,
            place_margin REAL,
            show_margin REAL,
            alternate_comment_line TEXT NOT NULL,
            weight INTEGER,
            odds REAL,
            entry TEXT NOT NULL,
            race_classication TEXT NOT NULL,
            claiming_price INTEGER,
            purse INTEGER,
            start_call_position TEXT NOT NULL,
            first_call_position TEXT NOT NULL,
            second_call_position TEXT NOT NULL,
            gate_call_position TEXT NOT NULL,
            stretch_call_position TEXT NOT NULL,
            finish_position TEXT NOT NULL,
            money_position TEXT NOT NULL,
            start_call_between_lengths_leader REAL,
            start_call_between_lengths REAL,
            first_call_between_lengths_leader REAL,
            first_call_between_lengths REAL,
            second_call_between_lengths_leader REAL,
            second_call_between_lengths REAL,
            bris_race_shape_1st_call INTEGER,
            stretch_call_between_lengths_leader REAL,
            stretch_call_between_lengths REAL,
            finish_between_lengths_leader REAL,
            finish_between_lengths REAL,
            bris_race_shape_2nd_call INTEGER,
            bris_2f_pace INTEGER,
            bris_4f_pace INTEGER,
            bris_6f_pace INTEGER,
            bris_8f_pace INTEGER,
            bris_10f_pace INTEGER,
            bris_late_pace INTEGER,
            bris_speed_rating INTEGER,
            speed_rating INTEGER,
            track_variant INTEGER,
            two_f_fraction REAL,
            three_f_fraction REAL,
            four_f_fraction REAL,
            five_f_fraction REAL,
            six_f_fraction REAL,
            seven_f_fraction REAL,
            eight_f_fraction REAL,
            ten_f_fraction REAL,
            twelve_f_fraction REAL,
            fourteen_f_fraction REAL,
            sixteen_f_fraction REAL,
            fraction_1 REAL,
            fraction_2 REAL,
            fraction_3 REAL,
            final_time REAL,
            claimed_code TEXT NOT NULL,
            trainer TEXT NOT NULL,
            jockey TEXT NOT NULL,
            apprentice_weight_allowance INTEGER,
            race_type TEXT NOT NULL,
            age_sex_restrictions TEXT NOT NULL,
            statebred_flag TEXT NOT NULL,
            restricted_qualifier_flag TEXT NOT NULL,
            favorite_indicator TEXT NOT NULL,
            front_bandages_indicator TEXT NOT NULL,
            bris_speed_par_for_race INTEGER,
            bar_shoes TEXT NOT NULL,
            company_line_codes TEXT NOT NULL,
            low_claiming_price_of_race INTEGER,
            high_claiming_price_of_race INTEGER,
            code_for_prior_races TEXT NOT NULL,
            claimed_and_trainer_switches_1 TEXT NOT NULL,
            claimed_and_trainer_switches_2 TEXT NOT NULL,
            claimed_and_trainer_switches_3 TEXT NOT NULL,
            claimed_and_trainer_switches_4 TEXT NOT NULL,
            claimed_and_trainer_switches_5 TEXT NOT NULL,
            claimed_and_trainer_switches_6 TEXT NOT NULL,
            extended_start_comment TEXT NOT NULL,
            sealed_track_indicator TEXT NOT NULL,
            previous_all_weather_surface_indicator TEXT NOT NULL,
            equibase_abbreviated_race_condition TEXT NOT NULL,
            FOREIGN KEY (horse_id) REFERENCES horses(id) ON DELETE CASCADE
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS key_trainer_stats (
            id INTEGER PRIMARY KEY,
            horse_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            starts INTEGER,
            win_pct REAL,
            in_the_money_pct REAL,
            roi REAL,
            FOREIGN KEY (horse_id) REFERENCES horses(id) ON DELETE CASCADE
        );
        "#,
    ];

    for statement in statements {
        sqlx::query(statement).execute(pool).await?;
    }

    Ok(())
}

pub async fn read_racecard(pool: &SqlitePool, racecard_row: SqliteRow) -> Result<Racecard, sqlx::Error> {
    let mut racecard = Racecard {
        id: racecard_row.get("id"),
        zip_file_name: racecard_row.get("zip_file_name"),
        track_code: racecard_row.get("track_code"),
        track: racecard_row.get("track"),
        date: racecard_row.get("date"),
        long_date: racecard_row.get("long_date"),
        races: Vec::new(),
    };

    let race_rows = sqlx::query("SELECT * FROM races WHERE racecard_id = ? ORDER BY id;")
        .bind(racecard.id)
        .fetch_all(pool)
        .await?;

    let horse_rows = sqlx::query(
        r#"
        SELECT h.* FROM horses h
        JOIN races r ON r.id = h.race_id
        WHERE r.racecard_id = ?
        ORDER BY h.id;
        "#,
    )
    .bind(racecard.id)
    .fetch_all(pool)
    .await?;

    let workout_rows = sqlx::query(
        r#"
        SELECT w.* FROM workouts w
        JOIN horses h ON h.id = w.horse_id
        JOIN races r ON r.id = h.race_id
        WHERE r.racecard_id = ?
        ORDER BY w.id;
        "#,
    )
    .bind(racecard.id)
    .fetch_all(pool)
    .await?;

    let past_performance_rows = sqlx::query(
        r#"
        SELECT pp.* FROM past_performances pp
        JOIN horses h ON h.id = pp.horse_id
        JOIN races r ON r.id = h.race_id
        WHERE r.racecard_id = ?
        ORDER BY pp.id;
        "#,
    )
    .bind(racecard.id)
    .fetch_all(pool)
    .await?;

    let key_trainer_rows = sqlx::query(
        r#"
        SELECT kts.* FROM key_trainer_stats kts
        JOIN horses h ON h.id = kts.horse_id
        JOIN races r ON r.id = h.race_id
        WHERE r.racecard_id = ?
        ORDER BY kts.id;
        "#,
    )
    .bind(racecard.id)
    .fetch_all(pool)
    .await?;

    let mut workouts_by_horse: HashMap<i64, Vec<Workout>> = HashMap::new();
    for row in workout_rows {
        let workout = workout_from_row(&row);
        workouts_by_horse
            .entry(workout.horse_id)
            .or_default()
            .push(workout);
    }

    let mut past_by_horse: HashMap<i64, Vec<PastPerformance>> = HashMap::new();
    for row in past_performance_rows {
        let past = past_performance_from_row(&row);
        past_by_horse.entry(past.horse_id).or_default().push(past);
    }

    let mut kts_by_horse: HashMap<i64, Vec<KeyTrainerStat>> = HashMap::new();
    for row in key_trainer_rows {
        let kts = key_trainer_stat_from_row(&row);
        kts_by_horse.entry(kts.horse_id).or_default().push(kts);
    }

    let mut horses_by_race: HashMap<i64, Vec<Horse>> = HashMap::new();
    for row in horse_rows {
        let mut horse = horse_from_row(&row);
        horse.workouts = workouts_by_horse.remove(&horse.id).unwrap_or_default();
        horse.past_performances = past_by_horse.remove(&horse.id).unwrap_or_default();
        horse.key_trainer_stats = kts_by_horse.remove(&horse.id).unwrap_or_default();
        horses_by_race.entry(horse.race_id).or_default().push(horse);
    }

    let mut races = Vec::with_capacity(race_rows.len());
    for row in race_rows {
        let mut race = race_from_row(&row);
        race.horses = horses_by_race.remove(&race.id).unwrap_or_default();
        races.push(race);
    }

    racecard.races = races;
    Ok(racecard)
}


fn opt_u32(row: &SqliteRow, col: &str) -> Option<u32> {
    row.try_get::<Option<i64>, _>(col).ok().flatten().and_then(|v| u32::try_from(v).ok())
}

fn opt_i32(row: &SqliteRow, col: &str) -> Option<i32> {
    row.try_get::<Option<i64>, _>(col).ok().flatten().and_then(|v| i32::try_from(v).ok())
}

fn opt_f64(row: &SqliteRow, col: &str) -> Option<f64> {
    row.try_get::<Option<f64>, _>(col).ok().flatten()
}

fn race_from_row(row: &SqliteRow) -> Race {
    Race {
        id: row.get("id"),
        racecard_id: row.get("racecard_id"),
        race_number: opt_u32(row, "race_number"),
        distance: opt_i32(row, "distance"),
        surface: row.get("surface"),
        race_type: row.get("race_type"),
        age_sex_restrictions: row.get("age_sex_restrictions"),
        todays_race_classification: row.get("todays_race_classification"),
        purse: opt_u32(row, "purse"),
        claiming_price: opt_u32(row, "claiming_price"),
        track_record: opt_f64(row, "track_record"),
        race_conditions: row.get("race_conditions"),
        todays_lasix_list: row.get("todays_lasix_list"),
        todays_bute_list: row.get("todays_bute_list"),
        todays_coupled_list: row.get("todays_coupled_list"),
        todays_mutuel_list: row.get("todays_mutuel_list"),
        simulcast_host_track_code: row.get("simulcast_host_track_code"),
        simulcast_host_track_race_number: opt_u32(row, "simulcast_host_track_race_number"),
        all_weather_surface_flag: row.get("all_weather_surface_flag"),
        race_conditions_line1: row.get("race_conditions_line1"),
        race_conditions_line2: row.get("race_conditions_line2"),
        race_conditions_line3: row.get("race_conditions_line3"),
        race_conditions_line4: row.get("race_conditions_line4"),
        race_conditions_line5: row.get("race_conditions_line5"),
        race_conditions_line6: row.get("race_conditions_line6"),
        low_claiming_price: opt_u32(row, "low_claiming_price"),
        statebred_flag: row.get("statebred_flag"),
        wager_type_line1: row.get("wager_type_line1"),
        wager_type_line2: row.get("wager_type_line2"),
        wager_type_line3: row.get("wager_type_line3"),
        wager_type_line4: row.get("wager_type_line4"),
        wager_type_line5: row.get("wager_type_line5"),
        wager_type_line6: row.get("wager_type_line6"),
        wager_type_line7: row.get("wager_type_line7"),
        wager_type_line8: row.get("wager_type_line8"),
        wager_type_line9: row.get("wager_type_line9"),
        two_f_bris_pace_par: opt_u32(row, "two_f_bris_pace_par"),
        four_f_bris_pace_par: opt_u32(row, "four_f_bris_pace_par"),
        six_f_bris_pace_par: opt_u32(row, "six_f_bris_pace_par"),
        bris_speed_for_class: opt_u32(row, "bris_speed_for_class"),
        bris_late_pace_par: opt_u32(row, "bris_late_pace_par"),
        post_times: row.get("post_times"),
        post_time_pacific_military: row.get("post_time_pacific_military"),
        todays_equibase_abbreviated_race_conditions: row.get("todays_equibase_abbreviated_race_conditions"),
        horses: Vec::new(),
    }
}

fn horse_from_row(row: &SqliteRow) -> Horse {
    Horse {
        id: row.get("id"),
        race_id: row.get("race_id"),
        scratched: row.get::<i64, _>("scratched") != 0,
        trip_handicapping_info: row.get("trip_handicapping_info"),
        post_position: opt_u32(row, "post_position"),
        entry: row.get("entry"),
        claiming_price_of_horse: opt_u32(row, "claiming_price_of_horse"),
        breed_type: row.get("breed_type"),
        todays_nasal_strip_change: opt_u32(row, "todays_nasal_strip_change"),
        todays_trainer: row.get("todays_trainer"),
        trainer_starts: opt_u32(row, "trainer_starts"),
        trainer_wins: opt_u32(row, "trainer_wins"),
        trainer_places: opt_u32(row, "trainer_places"),
        trainer_shows: opt_u32(row, "trainer_shows"),
        todays_jockey: row.get("todays_jockey"),
        apprentice_weight_allowance: opt_u32(row, "apprentice_weight_allowance"),
        jockey_starts: opt_u32(row, "jockey_starts"),
        jockey_wins: opt_u32(row, "jockey_wins"),
        jockey_places: opt_u32(row, "jockey_places"),
        jockey_shows: opt_u32(row, "jockey_shows"),
        todays_owner: row.get("todays_owner"),
        owners_silks: row.get("owners_silks"),
        main_track_only_ae_indicator: row.get("main_track_only_ae_indicator"),
        program_number: row.get("program_number"),
        morning_line_odds: opt_f64(row, "morning_line_odds"),
        horse_name: row.get("horse_name"),
        year_of_birth: opt_u32(row, "year_of_birth"),
        horses_foaling_month: opt_u32(row, "horses_foaling_month"),
        sex: row.get("sex"),
        horses_color: row.get("horses_color"),
        weight: opt_u32(row, "weight"),
        sire: row.get("sire"),
        sires_sire: row.get("sires_sire"),
        dam: row.get("dam"),
        dams_sire: row.get("dams_sire"),
        breeder: row.get("breeder"),
        state_country_where_bred: row.get("state_country_where_bred"),
        program_post_position: row.get("program_post_position"),
        todays_medication_new: opt_u32(row, "todays_medication_new"),
        todays_medication_old: opt_u32(row, "todays_medication_old"),
        equipment_change: opt_u32(row, "equipment_change"),
        lifetime_record_todays_distance_starts: opt_u32(row, "lifetime_record_todays_distance_starts"),
        lifetime_record_todays_distance_wins: opt_u32(row, "lifetime_record_todays_distance_wins"),
        lifetime_record_todays_distance_places: opt_u32(row, "lifetime_record_todays_distance_places"),
        lifetime_record_todays_distance_shows: opt_u32(row, "lifetime_record_todays_distance_shows"),
        lifetime_record_todays_distance_earnings: opt_u32(row, "lifetime_record_todays_distance_earnings"),
        lifetime_record_todays_track_starts: opt_u32(row, "lifetime_record_todays_track_starts"),
        lifetime_record_todays_track_wins: opt_u32(row, "lifetime_record_todays_track_wins"),
        lifetime_record_todays_track_places: opt_u32(row, "lifetime_record_todays_track_places"),
        lifetime_record_todays_track_shows: opt_u32(row, "lifetime_record_todays_track_shows"),
        lifetime_record_todays_track_earnings: opt_u32(row, "lifetime_record_todays_track_earnings"),
        lifetime_record_turf_starts: opt_u32(row, "lifetime_record_turf_starts"),
        lifetime_record_turf_wins: opt_u32(row, "lifetime_record_turf_wins"),
        lifetime_record_turf_places: opt_u32(row, "lifetime_record_turf_places"),
        lifetime_record_turf_shows: opt_u32(row, "lifetime_record_turf_shows"),
        lifetime_record_turf_earnings: opt_u32(row, "lifetime_record_turf_earnings"),
        lifetime_record_wet_starts: opt_u32(row, "lifetime_record_wet_starts"),
        lifetime_record_wet_wins: opt_u32(row, "lifetime_record_wet_wins"),
        lifetime_record_wet_places: opt_u32(row, "lifetime_record_wet_places"),
        lifetime_record_wet_shows: opt_u32(row, "lifetime_record_wet_shows"),
        lifetime_record_wet_earnings: opt_u32(row, "lifetime_record_wet_earnings"),
        current_year_record_year: opt_u32(row, "current_year_record_year"),
        current_year_record_starts: opt_u32(row, "current_year_record_starts"),
        current_year_record_wins: opt_u32(row, "current_year_record_wins"),
        current_year_record_places: opt_u32(row, "current_year_record_places"),
        current_year_record_shows: opt_u32(row, "current_year_record_shows"),
        current_year_record_earnings: opt_u32(row, "current_year_record_earnings"),
        previous_year_record_year: opt_u32(row, "previous_year_record_year"),
        previous_year_record_starts: opt_u32(row, "previous_year_record_starts"),
        previous_year_record_wins: opt_u32(row, "previous_year_record_wins"),
        previous_year_record_places: opt_u32(row, "previous_year_record_places"),
        previous_year_record_shows: opt_u32(row, "previous_year_record_shows"),
        previous_year_record_earnings: opt_u32(row, "previous_year_record_earnings"),
        lifetime_record_starts: opt_u32(row, "lifetime_record_starts"),
        lifetime_record_wins: opt_u32(row, "lifetime_record_wins"),
        lifetime_record_places: opt_u32(row, "lifetime_record_places"),
        lifetime_record_shows: opt_u32(row, "lifetime_record_shows"),
        lifetime_record_earnings: opt_u32(row, "lifetime_record_earnings"),
        bris_run_style: row.get("bris_run_style"),
        quirin_speed_points: opt_u32(row, "quirin_speed_points"),
        trainer_jockey_combo_starts: opt_u32(row, "trainer_jockey_combo_starts"),
        trainer_jockey_combo_wins: opt_u32(row, "trainer_jockey_combo_wins"),
        trainer_jockey_combo_places: opt_u32(row, "trainer_jockey_combo_places"),
        trainer_jockey_combo_shows: opt_u32(row, "trainer_jockey_combo_shows"),
        trainer_jockey_combo_roi: opt_f64(row, "trainer_jockey_combo_roi"),
        days_since_last_race: opt_u32(row, "days_since_last_race"),
        lifetime_all_weather_starts: opt_u32(row, "lifetime_all_weather_starts"),
        lifetime_all_weather_wins: opt_u32(row, "lifetime_all_weather_wins"),
        lifetime_all_weather_places: opt_u32(row, "lifetime_all_weather_places"),
        lifetime_all_weather_shows: opt_u32(row, "lifetime_all_weather_shows"),
        lifetime_all_weather_earnings: opt_u32(row, "lifetime_all_weather_earnings"),
        best_bris_speed_all_weather_surface: opt_u32(row, "best_bris_speed_all_weather_surface"),
        bris_prime_power_rating: opt_f64(row, "bris_prime_power_rating"),
        trainer_starts_current_year: opt_u32(row, "trainer_starts_current_year"),
        trainer_wins_current_year: opt_u32(row, "trainer_wins_current_year"),
        trainer_places_current_year: opt_u32(row, "trainer_places_current_year"),
        trainer_shows_current_year: opt_u32(row, "trainer_shows_current_year"),
        trainer_roi_current_year: opt_f64(row, "trainer_roi_current_year"),
        trainer_starts_previous_year: opt_u32(row, "trainer_starts_previous_year"),
        trainer_wins_previous_year: opt_u32(row, "trainer_wins_previous_year"),
        trainer_places_previous_year: opt_u32(row, "trainer_places_previous_year"),
        trainer_shows_previous_year: opt_u32(row, "trainer_shows_previous_year"),
        trainer_roi_previous_year: opt_f64(row, "trainer_roi_previous_year"),
        jockey_starts_current_year: opt_u32(row, "jockey_starts_current_year"),
        jockey_wins_current_year: opt_u32(row, "jockey_wins_current_year"),
        jockey_places_current_year: opt_u32(row, "jockey_places_current_year"),
        jockey_shows_current_year: opt_u32(row, "jockey_shows_current_year"),
        jockey_roi_current_year: opt_f64(row, "jockey_roi_current_year"),
        jockey_starts_previous_year: opt_u32(row, "jockey_starts_previous_year"),
        jockey_wins_previous_year: opt_u32(row, "jockey_wins_previous_year"),
        jockey_places_previous_year: opt_u32(row, "jockey_places_previous_year"),
        jockey_shows_previous_year: opt_u32(row, "jockey_shows_previous_year"),
        jockey_roi_previous_year: opt_f64(row, "jockey_roi_previous_year"),
        sire_stud_fee: opt_u32(row, "sire_stud_fee"),
        best_bris_speed_fast_track: opt_u32(row, "best_bris_speed_fast_track"),
        best_bris_speed_turf: opt_u32(row, "best_bris_speed_turf"),
        best_bris_speed_off_track: opt_u32(row, "best_bris_speed_off_track"),
        best_bris_speed_distance: opt_i32(row, "best_bris_speed_distance"),
        auction_price: opt_u32(row, "auction_price"),
        where_when_sold_at_auction: row.get("where_when_sold_at_auction"),
        bris_dirt_pedigree_rating: row.get("bris_dirt_pedigree_rating"),
        bris_mud_pedigree_rating: row.get("bris_mud_pedigree_rating"),
        bris_turf_pedigree_rating: row.get("bris_turf_pedigree_rating"),
        bris_distance_pedigree_rating: row.get("bris_distance_pedigree_rating"),
        best_bris_speed_life: opt_u32(row, "best_bris_speed_life"),
        best_bris_speed_most_recent_year: opt_u32(row, "best_bris_speed_most_recent_year"),
        best_bris_speed_2nd_most_recent_year: opt_u32(row, "best_bris_speed_2nd_most_recent_year"),
        best_bris_speed_todays_track: opt_u32(row, "best_bris_speed_todays_track"),
        starts_fast_dirt: opt_u32(row, "starts_fast_dirt"),
        wins_fast_dirt: opt_u32(row, "wins_fast_dirt"),
        places_fast_dirt: opt_u32(row, "places_fast_dirt"),
        shows_fast_dirt: opt_u32(row, "shows_fast_dirt"),
        earnings_fast_dirt: opt_u32(row, "earnings_fast_dirt"),
        jockey_distance_turf_label: row.get("jockey_distance_turf_label"),
        jockey_distance_turf_starts: opt_u32(row, "jockey_distance_turf_starts"),
        jockey_distance_turf_wins: opt_u32(row, "jockey_distance_turf_wins"),
        jockey_distance_turf_places: opt_u32(row, "jockey_distance_turf_places"),
        jockey_distance_turf_shows: opt_u32(row, "jockey_distance_turf_shows"),
        jockey_distance_turf_roi: opt_f64(row, "jockey_distance_turf_roi"),
        jockey_distance_turf_earnings: opt_u32(row, "jockey_distance_turf_earnings"),
        trainer_jockey_combo_starts_meet: opt_u32(row, "trainer_jockey_combo_starts_meet"),
        trainer_jockey_combo_wins_meet: opt_u32(row, "trainer_jockey_combo_wins_meet"),
        trainer_jockey_combo_places_meet: opt_u32(row, "trainer_jockey_combo_places_meet"),
        trainer_jockey_combo_shows_meet: opt_u32(row, "trainer_jockey_combo_shows_meet"),
        trainer_jockey_combo_roi_meet: opt_f64(row, "trainer_jockey_combo_roi_meet"),
        note: row.get("note"),
        workouts: Vec::new(),
        past_performances: Vec::new(),
        key_trainer_stats: Vec::new(),
    }
}

fn workout_from_row(row: &SqliteRow) -> Workout {
    Workout {
        id: row.get("id"),
        horse_id: row.get("horse_id"),
        date: row.get("date"),
        time: opt_f64(row, "time"),
        track: row.get("track"),
        distance: opt_i32(row, "distance"),
        condition: row.get("condition"),
        description: row.get("description"),
        main_inner_track_indicator: row.get("main_inner_track_indicator"),
        workouts_that_day_distance: opt_u32(row, "workouts_that_day_distance"),
        rank: opt_u32(row, "rank"),
    }
}

fn past_performance_from_row(row: &SqliteRow) -> PastPerformance {
    PastPerformance {
        id: row.get("id"),
        horse_id: row.get("horse_id"),
        race_date: row.get("race_date"),
        days_since_last_race: opt_u32(row, "days_since_last_race"),
        track_code: row.get("track_code"),
        bris_track_code: row.get("bris_track_code"),
        race_number: opt_u32(row, "race_number"),
        track_condition: row.get("track_condition"),
        distance: opt_i32(row, "distance"),
        surface: row.get("surface"),
        special_chute_indicator: row.get("special_chute_indicator"),
        entrants: opt_u32(row, "entrants"),
        post_position: opt_u32(row, "post_position"),
        equipment: row.get("equipment"),
        racename: row.get("racename"),
        medication: opt_u32(row, "medication"),
        trip_comment: row.get("trip_comment"),
        winners_name: row.get("winners_name"),
        place_name: row.get("place_name"),
        show_name: row.get("show_name"),
        winners_weight: opt_u32(row, "winners_weight"),
        place_weight: opt_u32(row, "place_weight"),
        show_weight: opt_u32(row, "show_weight"),
        winners_margin: opt_f64(row, "winners_margin"),
        place_margin: opt_f64(row, "place_margin"),
        show_margin: opt_f64(row, "show_margin"),
        alternate_comment_line: row.get("alternate_comment_line"),
        weight: opt_u32(row, "weight"),
        odds: opt_f64(row, "odds"),
        entry: row.get("entry"),
        race_classication: row.get("race_classication"),
        claiming_price: opt_u32(row, "claiming_price"),
        purse: opt_u32(row, "purse"),
        start_call_position: row.get("start_call_position"),
        first_call_position: row.get("first_call_position"),
        second_call_position: row.get("second_call_position"),
        gate_call_position: row.get("gate_call_position"),
        stretch_call_position: row.get("stretch_call_position"),
        finish_position: row.get("finish_position"),
        money_position: row.get("money_position"),
        start_call_between_lengths_leader: opt_f64(row, "start_call_between_lengths_leader"),
        start_call_between_lengths: opt_f64(row, "start_call_between_lengths"),
        first_call_between_lengths_leader: opt_f64(row, "first_call_between_lengths_leader"),
        first_call_between_lengths: opt_f64(row, "first_call_between_lengths"),
        second_call_between_lengths_leader: opt_f64(row, "second_call_between_lengths_leader"),
        second_call_between_lengths: opt_f64(row, "second_call_between_lengths"),
        bris_race_shape_1st_call: opt_u32(row, "bris_race_shape_1st_call"),
        stretch_call_between_lengths_leader: opt_f64(row, "stretch_call_between_lengths_leader"),
        stretch_call_between_lengths: opt_f64(row, "stretch_call_between_lengths"),
        finish_between_lengths_leader: opt_f64(row, "finish_between_lengths_leader"),
        finish_between_lengths: opt_f64(row, "finish_between_lengths"),
        bris_race_shape_2nd_call: opt_u32(row, "bris_race_shape_2nd_call"),
        bris_2f_pace: opt_u32(row, "bris_2f_pace"),
        bris_4f_pace: opt_u32(row, "bris_4f_pace"),
        bris_6f_pace: opt_u32(row, "bris_6f_pace"),
        bris_8f_pace: opt_u32(row, "bris_8f_pace"),
        bris_10f_pace: opt_u32(row, "bris_10f_pace"),
        bris_late_pace: opt_u32(row, "bris_late_pace"),
        bris_speed_rating: opt_u32(row, "bris_speed_rating"),
        speed_rating: opt_u32(row, "speed_rating"),
        track_variant: opt_i32(row, "track_variant"),
        two_f_fraction: opt_f64(row, "two_f_fraction"),
        three_f_fraction: opt_f64(row, "three_f_fraction"),
        four_f_fraction: opt_f64(row, "four_f_fraction"),
        five_f_fraction: opt_f64(row, "five_f_fraction"),
        six_f_fraction: opt_f64(row, "six_f_fraction"),
        seven_f_fraction: opt_f64(row, "seven_f_fraction"),
        eight_f_fraction: opt_f64(row, "eight_f_fraction"),
        ten_f_fraction: opt_f64(row, "ten_f_fraction"),
        twelve_f_fraction: opt_f64(row, "twelve_f_fraction"),
        fourteen_f_fraction: opt_f64(row, "fourteen_f_fraction"),
        sixteen_f_fraction: opt_f64(row, "sixteen_f_fraction"),
        fraction_1: opt_f64(row, "fraction_1"),
        fraction_2: opt_f64(row, "fraction_2"),
        fraction_3: opt_f64(row, "fraction_3"),
        final_time: opt_f64(row, "final_time"),
        claimed_code: row.get("claimed_code"),
        trainer: row.get("trainer"),
        jockey: row.get("jockey"),
        apprentice_weight_allowance: opt_u32(row, "apprentice_weight_allowance"),
        race_type: row.get("race_type"),
        age_sex_restrictions: row.get("age_sex_restrictions"),
        statebred_flag: row.get("statebred_flag"),
        restricted_qualifier_flag: row.get("restricted_qualifier_flag"),
        favorite_indicator: row.get("favorite_indicator"),
        front_bandages_indicator: row.get("front_bandages_indicator"),
        bris_speed_par_for_race: opt_u32(row, "bris_speed_par_for_race"),
        bar_shoes: row.get("bar_shoes"),
        company_line_codes: row.get("company_line_codes"),
        low_claiming_price_of_race: opt_u32(row, "low_claiming_price_of_race"),
        high_claiming_price_of_race: opt_u32(row, "high_claiming_price_of_race"),
        code_for_prior_races: row.get("code_for_prior_races"),
        claimed_and_trainer_switches_1: row.get("claimed_and_trainer_switches_1"),
        claimed_and_trainer_switches_2: row.get("claimed_and_trainer_switches_2"),
        claimed_and_trainer_switches_3: row.get("claimed_and_trainer_switches_3"),
        claimed_and_trainer_switches_4: row.get("claimed_and_trainer_switches_4"),
        claimed_and_trainer_switches_5: row.get("claimed_and_trainer_switches_5"),
        claimed_and_trainer_switches_6: row.get("claimed_and_trainer_switches_6"),
        extended_start_comment: row.get("extended_start_comment"),
        sealed_track_indicator: row.get("sealed_track_indicator"),
        previous_all_weather_surface_indicator: row.get("previous_all_weather_surface_indicator"),
        equibase_abbreviated_race_condition: row.get("equibase_abbreviated_race_condition"),
    }
}

fn key_trainer_stat_from_row(row: &SqliteRow) -> KeyTrainerStat {
    KeyTrainerStat {
        id: row.get("id"),
        horse_id: row.get("horse_id"),
        category: row.get("category"),
        starts: opt_u32(row, "starts"),
        win_pct: opt_f64(row, "win_pct"),
        in_the_money_pct: opt_f64(row, "in_the_money_pct"),
        roi: opt_f64(row, "roi"),
    }
}

pub async fn add_racecard(
    pool: &SqlitePool,
    mut racecard: Racecard
) -> Result<Racecard, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let result = sqlx::query(
        r#"
        INSERT INTO racecards (
            zip_file_name,
            track_code,
            track,
            date,
            long_date
        )
        VALUES (?, ?, ?, ?, ?);
        "#,
    )
    .bind(&racecard.zip_file_name)
    .bind(&racecard.track_code)
    .bind(&racecard.track)
    .bind(&racecard.date)
    .bind(&racecard.long_date)
    .execute(&mut *tx)
    .await?;
    racecard.id = result.last_insert_rowid();

    for race in &mut racecard.races {
        race.racecard_id = racecard.id;
        let race_sql = format!(
            r#"
            INSERT INTO races (
                racecard_id,
                race_number,
                distance,
                surface,
                race_type,
                age_sex_restrictions,
                todays_race_classification,
                purse,
                claiming_price,
                track_record,
                race_conditions,
                todays_lasix_list,
                todays_bute_list,
                todays_coupled_list,
                todays_mutuel_list,
                simulcast_host_track_code,
                simulcast_host_track_race_number,
                all_weather_surface_flag,
                race_conditions_line1,
                race_conditions_line2,
                race_conditions_line3,
                race_conditions_line4,
                race_conditions_line5,
                race_conditions_line6,
                low_claiming_price,
                statebred_flag,
                wager_type_line1,
                wager_type_line2,
                wager_type_line3,
                wager_type_line4,
                wager_type_line5,
                wager_type_line6,
                wager_type_line7,
                wager_type_line8,
                wager_type_line9,
                two_f_bris_pace_par,
                four_f_bris_pace_par,
                six_f_bris_pace_par,
                bris_speed_for_class,
                bris_late_pace_par,
                post_times,
                post_time_pacific_military,
                todays_equibase_abbreviated_race_conditions
            )
            VALUES ({});
            "#,
            placeholders(RACE_COLUMNS)
        );
        let result = sqlx::query(&race_sql)
            .bind(race.racecard_id)
            .bind(race.race_number)
            .bind(race.distance)
            .bind(&race.surface)
            .bind(&race.race_type)
            .bind(&race.age_sex_restrictions)
            .bind(&race.todays_race_classification)
            .bind(race.purse)
            .bind(race.claiming_price)
            .bind(race.track_record)
            .bind(&race.race_conditions)
            .bind(&race.todays_lasix_list)
            .bind(&race.todays_bute_list)
            .bind(&race.todays_coupled_list)
            .bind(&race.todays_mutuel_list)
            .bind(&race.simulcast_host_track_code)
            .bind(race.simulcast_host_track_race_number)
            .bind(&race.all_weather_surface_flag)
            .bind(&race.race_conditions_line1)
            .bind(&race.race_conditions_line2)
            .bind(&race.race_conditions_line3)
            .bind(&race.race_conditions_line4)
            .bind(&race.race_conditions_line5)
            .bind(&race.race_conditions_line6)
            .bind(race.low_claiming_price)
            .bind(&race.statebred_flag)
            .bind(&race.wager_type_line1)
            .bind(&race.wager_type_line2)
            .bind(&race.wager_type_line3)
            .bind(&race.wager_type_line4)
            .bind(&race.wager_type_line5)
            .bind(&race.wager_type_line6)
            .bind(&race.wager_type_line7)
            .bind(&race.wager_type_line8)
            .bind(&race.wager_type_line9)
            .bind(race.two_f_bris_pace_par)
            .bind(race.four_f_bris_pace_par)
            .bind(race.six_f_bris_pace_par)
            .bind(race.bris_speed_for_class)
            .bind(race.bris_late_pace_par)
            .bind(&race.post_times)
            .bind(&race.post_time_pacific_military)
            .bind(&race.todays_equibase_abbreviated_race_conditions)
            .execute(&mut *tx)
            .await?;
        race.id = result.last_insert_rowid();

        for horse in &mut race.horses {
            horse.race_id = race.id;
            let horse_sql = format!(
                r#"
                INSERT INTO horses (
                    race_id,
                    scratched,
                    trip_handicapping_info,
                    post_position,
                    entry,
                    claiming_price_of_horse,
                    breed_type,
                    todays_nasal_strip_change,
                    todays_trainer,
                    trainer_starts,
                    trainer_wins,
                    trainer_places,
                    trainer_shows,
                    todays_jockey,
                    apprentice_weight_allowance,
                    jockey_starts,
                    jockey_wins,
                    jockey_places,
                    jockey_shows,
                    todays_owner,
                    owners_silks,
                    main_track_only_ae_indicator,
                    program_number,
                    morning_line_odds,
                    horse_name,
                    year_of_birth,
                    horses_foaling_month,
                    sex,
                    horses_color,
                    weight,
                    sire,
                    sires_sire,
                    dam,
                    dams_sire,
                    breeder,
                    state_country_where_bred,
                    program_post_position,
                    todays_medication_new,
                    todays_medication_old,
                    equipment_change,
                    lifetime_record_todays_distance_starts,
                    lifetime_record_todays_distance_wins,
                    lifetime_record_todays_distance_places,
                    lifetime_record_todays_distance_shows,
                    lifetime_record_todays_distance_earnings,
                    lifetime_record_todays_track_starts,
                    lifetime_record_todays_track_wins,
                    lifetime_record_todays_track_places,
                    lifetime_record_todays_track_shows,
                    lifetime_record_todays_track_earnings,
                    lifetime_record_turf_starts,
                    lifetime_record_turf_wins,
                    lifetime_record_turf_places,
                    lifetime_record_turf_shows,
                    lifetime_record_turf_earnings,
                    lifetime_record_wet_starts,
                    lifetime_record_wet_wins,
                    lifetime_record_wet_places,
                    lifetime_record_wet_shows,
                    lifetime_record_wet_earnings,
                    current_year_record_year,
                    current_year_record_starts,
                    current_year_record_wins,
                    current_year_record_places,
                    current_year_record_shows,
                    current_year_record_earnings,
                    previous_year_record_year,
                    previous_year_record_starts,
                    previous_year_record_wins,
                    previous_year_record_places,
                    previous_year_record_shows,
                    previous_year_record_earnings,
                    lifetime_record_starts,
                    lifetime_record_wins,
                    lifetime_record_places,
                    lifetime_record_shows,
                    lifetime_record_earnings,
                    bris_run_style,
                    quirin_speed_points,
                    trainer_jockey_combo_starts,
                    trainer_jockey_combo_wins,
                    trainer_jockey_combo_places,
                    trainer_jockey_combo_shows,
                    trainer_jockey_combo_roi,
                    days_since_last_race,
                    lifetime_all_weather_starts,
                    lifetime_all_weather_wins,
                    lifetime_all_weather_places,
                    lifetime_all_weather_shows,
                    lifetime_all_weather_earnings,
                    best_bris_speed_all_weather_surface,
                    bris_prime_power_rating,
                    trainer_starts_current_year,
                    trainer_wins_current_year,
                    trainer_places_current_year,
                    trainer_shows_current_year,
                    trainer_roi_current_year,
                    trainer_starts_previous_year,
                    trainer_wins_previous_year,
                    trainer_places_previous_year,
                    trainer_shows_previous_year,
                    trainer_roi_previous_year,
                    jockey_starts_current_year,
                    jockey_wins_current_year,
                    jockey_places_current_year,
                    jockey_shows_current_year,
                    jockey_roi_current_year,
                    jockey_starts_previous_year,
                    jockey_wins_previous_year,
                    jockey_places_previous_year,
                    jockey_shows_previous_year,
                    jockey_roi_previous_year,
                    sire_stud_fee,
                    best_bris_speed_fast_track,
                    best_bris_speed_turf,
                    best_bris_speed_off_track,
                    best_bris_speed_distance,
                    auction_price,
                    where_when_sold_at_auction,
                    bris_dirt_pedigree_rating,
                    bris_mud_pedigree_rating,
                    bris_turf_pedigree_rating,
                    bris_distance_pedigree_rating,
                    best_bris_speed_life,
                    best_bris_speed_most_recent_year,
                    best_bris_speed_2nd_most_recent_year,
                    best_bris_speed_todays_track,
                    starts_fast_dirt,
                    wins_fast_dirt,
                    places_fast_dirt,
                    shows_fast_dirt,
                    earnings_fast_dirt,
                    jockey_distance_turf_label,
                    jockey_distance_turf_starts,
                    jockey_distance_turf_wins,
                    jockey_distance_turf_places,
                    jockey_distance_turf_shows,
                    jockey_distance_turf_roi,
                    jockey_distance_turf_earnings,
                    trainer_jockey_combo_starts_meet,
                    trainer_jockey_combo_wins_meet,
                    trainer_jockey_combo_places_meet,
                    trainer_jockey_combo_shows_meet,
                    trainer_jockey_combo_roi_meet,
                    note
                )
                VALUES ({});
                "#,
                placeholders(HORSE_COLUMNS)
            );
            let result = sqlx::query(&horse_sql)
                .bind(horse.race_id)
                .bind(horse.scratched)
                .bind(&horse.trip_handicapping_info)
                .bind(horse.post_position)
                .bind(&horse.entry)
                .bind(horse.claiming_price_of_horse)
                .bind(&horse.breed_type)
                .bind(horse.todays_nasal_strip_change)
                .bind(&horse.todays_trainer)
                .bind(horse.trainer_starts)
                .bind(horse.trainer_wins)
                .bind(horse.trainer_places)
                .bind(horse.trainer_shows)
                .bind(&horse.todays_jockey)
                .bind(horse.apprentice_weight_allowance)
                .bind(horse.jockey_starts)
                .bind(horse.jockey_wins)
                .bind(horse.jockey_places)
                .bind(horse.jockey_shows)
                .bind(&horse.todays_owner)
                .bind(&horse.owners_silks)
                .bind(&horse.main_track_only_ae_indicator)
                .bind(&horse.program_number)
                .bind(horse.morning_line_odds)
                .bind(&horse.horse_name)
                .bind(horse.year_of_birth)
                .bind(horse.horses_foaling_month)
                .bind(&horse.sex)
                .bind(&horse.horses_color)
                .bind(horse.weight)
                .bind(&horse.sire)
                .bind(&horse.sires_sire)
                .bind(&horse.dam)
                .bind(&horse.dams_sire)
                .bind(&horse.breeder)
                .bind(&horse.state_country_where_bred)
                .bind(&horse.program_post_position)
                .bind(horse.todays_medication_new)
                .bind(horse.todays_medication_old)
                .bind(horse.equipment_change)
                .bind(horse.lifetime_record_todays_distance_starts)
                .bind(horse.lifetime_record_todays_distance_wins)
                .bind(horse.lifetime_record_todays_distance_places)
                .bind(horse.lifetime_record_todays_distance_shows)
                .bind(horse.lifetime_record_todays_distance_earnings)
                .bind(horse.lifetime_record_todays_track_starts)
                .bind(horse.lifetime_record_todays_track_wins)
                .bind(horse.lifetime_record_todays_track_places)
                .bind(horse.lifetime_record_todays_track_shows)
                .bind(horse.lifetime_record_todays_track_earnings)
                .bind(horse.lifetime_record_turf_starts)
                .bind(horse.lifetime_record_turf_wins)
                .bind(horse.lifetime_record_turf_places)
                .bind(horse.lifetime_record_turf_shows)
                .bind(horse.lifetime_record_turf_earnings)
                .bind(horse.lifetime_record_wet_starts)
                .bind(horse.lifetime_record_wet_wins)
                .bind(horse.lifetime_record_wet_places)
                .bind(horse.lifetime_record_wet_shows)
                .bind(horse.lifetime_record_wet_earnings)
                .bind(horse.current_year_record_year)
                .bind(horse.current_year_record_starts)
                .bind(horse.current_year_record_wins)
                .bind(horse.current_year_record_places)
                .bind(horse.current_year_record_shows)
                .bind(horse.current_year_record_earnings)
                .bind(horse.previous_year_record_year)
                .bind(horse.previous_year_record_starts)
                .bind(horse.previous_year_record_wins)
                .bind(horse.previous_year_record_places)
                .bind(horse.previous_year_record_shows)
                .bind(horse.previous_year_record_earnings)
                .bind(horse.lifetime_record_starts)
                .bind(horse.lifetime_record_wins)
                .bind(horse.lifetime_record_places)
                .bind(horse.lifetime_record_shows)
                .bind(horse.lifetime_record_earnings)
                .bind(&horse.bris_run_style)
                .bind(horse.quirin_speed_points)
                .bind(horse.trainer_jockey_combo_starts)
                .bind(horse.trainer_jockey_combo_wins)
                .bind(horse.trainer_jockey_combo_places)
                .bind(horse.trainer_jockey_combo_shows)
                .bind(horse.trainer_jockey_combo_roi)
                .bind(horse.days_since_last_race)
                .bind(horse.lifetime_all_weather_starts)
                .bind(horse.lifetime_all_weather_wins)
                .bind(horse.lifetime_all_weather_places)
                .bind(horse.lifetime_all_weather_shows)
                .bind(horse.lifetime_all_weather_earnings)
                .bind(horse.best_bris_speed_all_weather_surface)
                .bind(horse.bris_prime_power_rating)
                .bind(horse.trainer_starts_current_year)
                .bind(horse.trainer_wins_current_year)
                .bind(horse.trainer_places_current_year)
                .bind(horse.trainer_shows_current_year)
                .bind(horse.trainer_roi_current_year)
                .bind(horse.trainer_starts_previous_year)
                .bind(horse.trainer_wins_previous_year)
                .bind(horse.trainer_places_previous_year)
                .bind(horse.trainer_shows_previous_year)
                .bind(horse.trainer_roi_previous_year)
                .bind(horse.jockey_starts_current_year)
                .bind(horse.jockey_wins_current_year)
                .bind(horse.jockey_places_current_year)
                .bind(horse.jockey_shows_current_year)
                .bind(horse.jockey_roi_current_year)
                .bind(horse.jockey_starts_previous_year)
                .bind(horse.jockey_wins_previous_year)
                .bind(horse.jockey_places_previous_year)
                .bind(horse.jockey_shows_previous_year)
                .bind(horse.jockey_roi_previous_year)
                .bind(horse.sire_stud_fee)
                .bind(horse.best_bris_speed_fast_track)
                .bind(horse.best_bris_speed_turf)
                .bind(horse.best_bris_speed_off_track)
                .bind(horse.best_bris_speed_distance)
                .bind(horse.auction_price)
                .bind(&horse.where_when_sold_at_auction)
                .bind(&horse.bris_dirt_pedigree_rating)
                .bind(&horse.bris_mud_pedigree_rating)
                .bind(&horse.bris_turf_pedigree_rating)
                .bind(&horse.bris_distance_pedigree_rating)
                .bind(horse.best_bris_speed_life)
                .bind(horse.best_bris_speed_most_recent_year)
                .bind(horse.best_bris_speed_2nd_most_recent_year)
                .bind(horse.best_bris_speed_todays_track)
                .bind(horse.starts_fast_dirt)
                .bind(horse.wins_fast_dirt)
                .bind(horse.places_fast_dirt)
                .bind(horse.shows_fast_dirt)
                .bind(horse.earnings_fast_dirt)
                .bind(&horse.jockey_distance_turf_label)
                .bind(horse.jockey_distance_turf_starts)
                .bind(horse.jockey_distance_turf_wins)
                .bind(horse.jockey_distance_turf_places)
                .bind(horse.jockey_distance_turf_shows)
                .bind(horse.jockey_distance_turf_roi)
                .bind(horse.jockey_distance_turf_earnings)
                .bind(horse.trainer_jockey_combo_starts_meet)
                .bind(horse.trainer_jockey_combo_wins_meet)
                .bind(horse.trainer_jockey_combo_places_meet)
                .bind(horse.trainer_jockey_combo_shows_meet)
                .bind(horse.trainer_jockey_combo_roi_meet)
                .bind(&horse.note)
                .execute(&mut *tx)
                .await?;
            horse.id = result.last_insert_rowid();

            for workout in &mut horse.workouts {
                workout.horse_id = horse.id;
                let result = sqlx::query(
                    r#"
                    INSERT INTO workouts (
                        horse_id,
                        date,
                        time,
                        track,
                        distance,
                        condition,
                        description,
                        main_inner_track_indicator,
                        workouts_that_day_distance,
                        rank
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    "#,
                )
                .bind(workout.horse_id)
                .bind(&workout.date)
                .bind(workout.time)
                .bind(&workout.track)
                .bind(workout.distance)
                .bind(&workout.condition)
                .bind(&workout.description)
                .bind(&workout.main_inner_track_indicator)
                .bind(workout.workouts_that_day_distance)
                .bind(workout.rank)
                .execute(&mut *tx)
                .await?;
                workout.id = result.last_insert_rowid();
            }

            for past_performance in &mut horse.past_performances {
                past_performance.horse_id = horse.id;
                let past_sql = format!(
                    r#"
                    INSERT INTO past_performances (
                        horse_id,
                        race_date,
                        days_since_last_race,
                        track_code,
                        bris_track_code,
                        race_number,
                        track_condition,
                        distance,
                        surface,
                        special_chute_indicator,
                        entrants,
                        post_position,
                        equipment,
                        racename,
                        medication,
                        trip_comment,
                        winners_name,
                        place_name,
                        show_name,
                        winners_weight,
                        place_weight,
                        show_weight,
                        winners_margin,
                        place_margin,
                        show_margin,
                        alternate_comment_line,
                        weight,
                        odds,
                        entry,
                        race_classication,
                        claiming_price,
                        purse,
                        start_call_position,
                        first_call_position,
                        second_call_position,
                        gate_call_position,
                        stretch_call_position,
                        finish_position,
                        money_position,
                        start_call_between_lengths_leader,
                        start_call_between_lengths,
                        first_call_between_lengths_leader,
                        first_call_between_lengths,
                        second_call_between_lengths_leader,
                        second_call_between_lengths,
                        bris_race_shape_1st_call,
                        stretch_call_between_lengths_leader,
                        stretch_call_between_lengths,
                        finish_between_lengths_leader,
                        finish_between_lengths,
                        bris_race_shape_2nd_call,
                        bris_2f_pace,
                        bris_4f_pace,
                        bris_6f_pace,
                        bris_8f_pace,
                        bris_10f_pace,
                        bris_late_pace,
                        bris_speed_rating,
                        speed_rating,
                        track_variant,
                        two_f_fraction,
                        three_f_fraction,
                        four_f_fraction,
                        five_f_fraction,
                        six_f_fraction,
                        seven_f_fraction,
                        eight_f_fraction,
                        ten_f_fraction,
                        twelve_f_fraction,
                        fourteen_f_fraction,
                        sixteen_f_fraction,
                        fraction_1,
                        fraction_2,
                        fraction_3,
                        final_time,
                        claimed_code,
                        trainer,
                        jockey,
                        apprentice_weight_allowance,
                        race_type,
                        age_sex_restrictions,
                        statebred_flag,
                        restricted_qualifier_flag,
                        favorite_indicator,
                        front_bandages_indicator,
                        bris_speed_par_for_race,
                        bar_shoes,
                        company_line_codes,
                        low_claiming_price_of_race,
                        high_claiming_price_of_race,
                        code_for_prior_races,
                        claimed_and_trainer_switches_1,
                        claimed_and_trainer_switches_2,
                        claimed_and_trainer_switches_3,
                        claimed_and_trainer_switches_4,
                        claimed_and_trainer_switches_5,
                        claimed_and_trainer_switches_6,
                        extended_start_comment,
                        sealed_track_indicator,
                        previous_all_weather_surface_indicator,
                        equibase_abbreviated_race_condition
                    )
                    VALUES ({});
                    "#,
                    placeholders(PAST_PERFORMANCE_COLUMNS)
                );
                let result = sqlx::query(&past_sql)
                    .bind(past_performance.horse_id)
                    .bind(&past_performance.race_date)
                    .bind(past_performance.days_since_last_race)
                    .bind(&past_performance.track_code)
                    .bind(&past_performance.bris_track_code)
                    .bind(past_performance.race_number)
                    .bind(&past_performance.track_condition)
                    .bind(past_performance.distance)
                    .bind(&past_performance.surface)
                    .bind(&past_performance.special_chute_indicator)
                    .bind(past_performance.entrants)
                    .bind(past_performance.post_position)
                    .bind(&past_performance.equipment)
                    .bind(&past_performance.racename)
                    .bind(past_performance.medication)
                    .bind(&past_performance.trip_comment)
                    .bind(&past_performance.winners_name)
                    .bind(&past_performance.place_name)
                    .bind(&past_performance.show_name)
                    .bind(past_performance.winners_weight)
                    .bind(past_performance.place_weight)
                    .bind(past_performance.show_weight)
                    .bind(past_performance.winners_margin)
                    .bind(past_performance.place_margin)
                    .bind(past_performance.show_margin)
                    .bind(&past_performance.alternate_comment_line)
                    .bind(past_performance.weight)
                    .bind(past_performance.odds)
                    .bind(&past_performance.entry)
                    .bind(&past_performance.race_classication)
                    .bind(past_performance.claiming_price)
                    .bind(past_performance.purse)
                    .bind(&past_performance.start_call_position)
                    .bind(&past_performance.first_call_position)
                    .bind(&past_performance.second_call_position)
                    .bind(&past_performance.gate_call_position)
                    .bind(&past_performance.stretch_call_position)
                    .bind(&past_performance.finish_position)
                    .bind(&past_performance.money_position)
                    .bind(past_performance.start_call_between_lengths_leader)
                    .bind(past_performance.start_call_between_lengths)
                    .bind(past_performance.first_call_between_lengths_leader)
                    .bind(past_performance.first_call_between_lengths)
                    .bind(past_performance.second_call_between_lengths_leader)
                    .bind(past_performance.second_call_between_lengths)
                    .bind(past_performance.bris_race_shape_1st_call)
                    .bind(past_performance.stretch_call_between_lengths_leader)
                    .bind(past_performance.stretch_call_between_lengths)
                    .bind(past_performance.finish_between_lengths_leader)
                    .bind(past_performance.finish_between_lengths)
                    .bind(past_performance.bris_race_shape_2nd_call)
                    .bind(past_performance.bris_2f_pace)
                    .bind(past_performance.bris_4f_pace)
                    .bind(past_performance.bris_6f_pace)
                    .bind(past_performance.bris_8f_pace)
                    .bind(past_performance.bris_10f_pace)
                    .bind(past_performance.bris_late_pace)
                    .bind(past_performance.bris_speed_rating)
                    .bind(past_performance.speed_rating)
                    .bind(past_performance.track_variant)
                    .bind(past_performance.two_f_fraction)
                    .bind(past_performance.three_f_fraction)
                    .bind(past_performance.four_f_fraction)
                    .bind(past_performance.five_f_fraction)
                    .bind(past_performance.six_f_fraction)
                    .bind(past_performance.seven_f_fraction)
                    .bind(past_performance.eight_f_fraction)
                    .bind(past_performance.ten_f_fraction)
                    .bind(past_performance.twelve_f_fraction)
                    .bind(past_performance.fourteen_f_fraction)
                    .bind(past_performance.sixteen_f_fraction)
                    .bind(past_performance.fraction_1)
                    .bind(past_performance.fraction_2)
                    .bind(past_performance.fraction_3)
                    .bind(past_performance.final_time)
                    .bind(&past_performance.claimed_code)
                    .bind(&past_performance.trainer)
                    .bind(&past_performance.jockey)
                    .bind(past_performance.apprentice_weight_allowance)
                    .bind(&past_performance.race_type)
                    .bind(&past_performance.age_sex_restrictions)
                    .bind(&past_performance.statebred_flag)
                    .bind(&past_performance.restricted_qualifier_flag)
                    .bind(&past_performance.favorite_indicator)
                    .bind(&past_performance.front_bandages_indicator)
                    .bind(past_performance.bris_speed_par_for_race)
                    .bind(&past_performance.bar_shoes)
                    .bind(&past_performance.company_line_codes)
                    .bind(past_performance.low_claiming_price_of_race)
                    .bind(past_performance.high_claiming_price_of_race)
                    .bind(&past_performance.code_for_prior_races)
                    .bind(&past_performance.claimed_and_trainer_switches_1)
                    .bind(&past_performance.claimed_and_trainer_switches_2)
                    .bind(&past_performance.claimed_and_trainer_switches_3)
                    .bind(&past_performance.claimed_and_trainer_switches_4)
                    .bind(&past_performance.claimed_and_trainer_switches_5)
                    .bind(&past_performance.claimed_and_trainer_switches_6)
                    .bind(&past_performance.extended_start_comment)
                    .bind(&past_performance.sealed_track_indicator)
                    .bind(&past_performance.previous_all_weather_surface_indicator)
                    .bind(&past_performance.equibase_abbreviated_race_condition)
                    .execute(&mut *tx)
                    .await?;
                past_performance.id = result.last_insert_rowid();
            }

            for key_trainer_stat in &mut horse.key_trainer_stats {
                key_trainer_stat.horse_id = horse.id;
                let result = sqlx::query(
                    r#"
                    INSERT INTO key_trainer_stats (
                        horse_id,
                        category,
                        starts,
                        win_pct,
                        in_the_money_pct,
                        roi
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                    "#,
                )
                .bind(key_trainer_stat.horse_id)
                .bind(&key_trainer_stat.category)
                .bind(key_trainer_stat.starts)
                .bind(key_trainer_stat.win_pct)
                .bind(key_trainer_stat.in_the_money_pct)
                .bind(key_trainer_stat.roi)
                .execute(&mut *tx)
                .await?;
                key_trainer_stat.id = result.last_insert_rowid();
            }
        }
    }

    tx.commit().await?;
    Ok(racecard)
}
