use crate::constants::single_file_indexes::*;
use crate::models::racecard::{Horse, KeyTrainerStat, PastPerformance, Race, Racecard, Workout};
use crate::utils::transformers::Transformers;
use tokio::fs;

const NUMBER_OF_COLUMNS: usize = 1435;

pub async fn build_racecard(path: String, zip_file_name: String) -> Result<Racecard, String> {
    let contents = fs::read_to_string(&path)
        .await
        .map_err(|e| format!("Failed to read racecard file: {}", e))?;

    let lines: Vec<Vec<String>> = contents
        .lines()
        .map(|line| {
            line.split(',')
                .take(NUMBER_OF_COLUMNS)
                .map(|field| field.trim().trim_matches('"').trim().to_string())
                .collect()
        })
        .collect();

    for (i, line) in lines.iter().enumerate() {
        if line.len() != NUMBER_OF_COLUMNS {
            return Err(format!("Line {} has {} columns, expected {}", i + 1, line.len(), NUMBER_OF_COLUMNS));
        }
    }

    let track_code = &lines[0][SF_TRACK];
    let track_name = track_code;

    let mut races = Vec::<Race>::new();

    for line in &lines {
        let race_number = line[SF_RACE_NUMBER].parse::<u32>().ok();

        let race_index = races.iter().position(|r| r.race_number == race_number);

        let race_idx = if let Some(idx) = race_index {
            idx
        } else {
            let race = Race {
                id: 0,
                racecard_id: 0,
                race_number: race_number,
                distance: line[SF_DISTANCE].parse::<i32>().ok(),
                surface: line[SF_SURFACE].clone(),
                race_type: line[SF_RACE_TYPE].clone(),
                age_sex_restrictions: line[SF_AGE_SEX_RESTRICTIONS].clone(),
                todays_race_classification: line[SF_TODAYS_RACE_CLASSIFICATION].clone(),
                purse: line[SF_PURSE].parse::<u32>().ok(),
                claiming_price: line[SF_CLAIMING_PRICE].parse::<u32>().ok(),
                track_record: line[SF_TRACK_RECORD].parse::<f64>().ok(),
                race_conditions: line[SF_RACE_CONDITIONS].clone(),
                todays_lasix_list: line[SF_TODAYS_LASIX_LIST].clone(),
                todays_bute_list: line[SF_TODAYS_BUTE_LIST].clone(),
                todays_coupled_list: line[SF_TODAYS_COUPLED_LIST].clone(),
                todays_mutuel_list: line[SF_TODAYS_MUTUEL_LIST].clone(),
                simulcast_host_track_code: line[SF_SIMULCAST_HOST_TRACK_CODE].clone(),
                simulcast_host_track_race_number: line[SF_SIMULCAST_HOST_TRACK_RACE_NUMBER]
                    .parse::<u32>().ok(),
                all_weather_surface_flag: line[SF_ALL_WEATHER_SURFACE_FLAG].clone(),
                race_conditions_line1: line[SF_RACE_CONDITIONS_LINE1].clone(),
                race_conditions_line2: line[SF_RACE_CONDITIONS_LINE2].clone(),
                race_conditions_line3: line[SF_RACE_CONDITIONS_LINE3].clone(),
                race_conditions_line4: line[SF_RACE_CONDITIONS_LINE4].clone(),
                race_conditions_line5: line[SF_RACE_CONDITIONS_LINE5].clone(),
                race_conditions_line6: line[SF_RACE_CONDITIONS_LINE6].clone(),
                low_claiming_price: line[SF_LOW_CLAIMING_PRICE].parse::<u32>().ok(),
                statebred_flag: line[SF_STATEBRED_FLAG].clone(),
                wager_type_line1: line[SF_WAGER_TYPE_LINE1].clone(),
                wager_type_line2: line[SF_WAGER_TYPE_LINE2].clone(),
                wager_type_line3: line[SF_WAGER_TYPE_LINE3].clone(),
                wager_type_line4: line[SF_WAGER_TYPE_LINE4].clone(),
                wager_type_line5: line[SF_WAGER_TYPE_LINE5].clone(),
                wager_type_line6: line[SF_WAGER_TYPE_LINE6].clone(),
                wager_type_line7: line[SF_WAGER_TYPE_LINE7].clone(),
                wager_type_line8: line[SF_WAGER_TYPE_LINE8].clone(),
                wager_type_line9: line[SF_WAGER_TYPE_LINE9].clone(),
                two_f_bris_pace_par: line[SF_TWO_F_BRIS_PACE_PAR].parse::<u32>().ok(),
                four_f_bris_pace_par: line[SF_FOUR_F_BRIS_PACE_PAR].parse::<u32>().ok(),
                six_f_bris_pace_par: line[SF_SIX_F_BRIS_PACE_PAR].parse::<u32>().ok(),
                bris_speed_for_class: line[SF_BRIS_SPEED_FOR_CLASS].parse::<u32>().ok(),
                bris_late_pace_par: line[SF_BRIS_LATE_PACE_PAR].parse::<u32>().ok(),
                post_times: line[SF_POST_TIMES].clone(),
                post_time_pacific_military: line[SF_POST_TIME_PACIFIC_MILITARY].clone(),
                todays_equibase_abbreviated_race_conditions: line
                [SF_TODAYS_EQUIBASE_ABBREVIATED_RACE_CONDITIONS]
                .clone(),
                horses: Vec::new(),
            };
            races.push(race);
            races.len() - 1
        };

        let mut horse = Horse {
            id: 0,
            race_id: 0,
            scratched: false,
            post_position: line[SF_POST_POSITION].parse::<u32>().ok(),
            entry: line[SF_ENTRY].clone(),
            claiming_price_of_horse: line[SF_CLAIMING_PRICE_OF_HORSE].parse::<u32>().ok(),
            breed_type: line[SF_BREED_TYPE].clone(),
            todays_nasal_strip_change: line[SF_TODAYS_NASAL_STRIP_CHANGE].parse::<u32>().ok(),
            todays_trainer: line[SF_TODAYS_TRAINER].clone(),
            trainer_starts: line[SF_TRAINER_STARTS].parse::<u32>().ok(),
            trainer_wins: line[SF_TRAINER_WINS].parse::<u32>().ok(),
            trainer_places: line[SF_TRAINER_PLACES].parse::<u32>().ok(),
            trainer_shows: line[SF_TRAINER_SHOWS].parse::<u32>().ok(),
            todays_jockey: line[SF_TODAYS_JOCKEY].clone(),
            apprentice_weight_allowance: line[SF_APPRENTICE_WEIGHT_ALLOWANCE].parse::<u32>().ok(),
            jockey_starts: line[SF_JOCKEY_STARTS].parse::<u32>().ok(),
            jockey_wins: line[SF_JOCKEY_WINS].parse::<u32>().ok(),
            jockey_places: line[SF_JOCKEY_PLACES].parse::<u32>().ok(),
            jockey_shows: line[SF_JOCKEY_SHOWS].parse::<u32>().ok(),
            todays_owner: line[SF_TODAYS_OWNER].clone(),
            owners_silks: line[SF_OWNERS_SILKS].clone(),
            main_track_only_ae_indicator: line[SF_MAIN_TRACK_ONLY_AE_INDICATOR].clone(),
            program_number: line[SF_PROGRAM_NUMBER].clone(),
            morning_line_odds: line[SF_MORNING_LINE_ODDS].parse::<f64>().ok(),
            horse_name: line[SF_HORSE_NAME].clone(),
            year_of_birth: line[SF_YEAR_OF_BIRTH].parse::<u32>().ok(),
            horses_foaling_month: line[SF_HORSES_FOALING_MONTH].parse::<u32>().ok(),
            sex: line[SF_SEX].clone(),
            horses_color: line[SF_HORSES_COLOR].clone(),
            weight: line[SF_WEIGHT].parse::<u32>().ok(),
            sire: line[SF_SIRE].clone(),
            sires_sire: line[SF_SIRES_SIRE].clone(),
            dam: line[SF_DAM].clone(),
            dams_sire: line[SF_DAMS_SIRE].clone(),
            breeder: line[SF_BREEDER].clone(),
            state_country_where_bred: line[SF_STATE_COUNTRY_WHERE_BRED].clone(),
            program_post_position: line[SF_PROGRAM_POST_POSITION].clone(),
            todays_medication_new: line[SF_TODAYS_MEDICATION_NEW].parse::<u32>().ok(),
            todays_medication_old: line[SF_TODAYS_MEDICATION_OLD].parse::<u32>().ok(),
            equipment_change: line[SF_EQUIPMENT_CHANGE].parse::<u32>().ok(),
            lifetime_record_todays_distance_starts: line[SF_LIFETIME_RECORD_TODAYS_DISTANCE_STARTS]
                .parse::<u32>().ok(),
            lifetime_record_todays_distance_wins: line[SF_LIFETIME_RECORD_TODAYS_DISTANCE_WINS]
                .parse::<u32>().ok(),
            lifetime_record_todays_distance_places: line[SF_LIFETIME_RECORD_TODAYS_DISTANCE_PLACES]
                .parse::<u32>().ok(),
            lifetime_record_todays_distance_shows: line[SF_LIFETIME_RECORD_TODAYS_DISTANCE_SHOWS]
                .parse::<u32>().ok(),
            lifetime_record_todays_distance_earnings: line
                [SF_LIFETIME_RECORD_TODAYS_DISTANCE_EARNINGS]
                .parse::<u32>().ok(),
            lifetime_record_todays_track_starts: line[SF_LIFETIME_RECORD_TODAYS_TRACK_STARTS]
                .parse::<u32>().ok(),
            lifetime_record_todays_track_wins: line[SF_LIFETIME_RECORD_TODAYS_TRACK_WINS]
                .parse::<u32>().ok(),
            lifetime_record_todays_track_places: line[SF_LIFETIME_RECORD_TODAYS_TRACK_PLACES]
                .parse::<u32>().ok(),
            lifetime_record_todays_track_shows: line[SF_LIFETIME_RECORD_TODAYS_TRACK_SHOWS]
                .parse::<u32>().ok(),
            lifetime_record_todays_track_earnings: line[SF_LIFETIME_RECORD_TODAYS_TRACK_EARNINGS]
                .parse::<u32>().ok(),
            lifetime_record_turf_starts: line[SF_LIFETIME_RECORD_TURF_STARTS].parse::<u32>().ok(),
            lifetime_record_turf_wins: line[SF_LIFETIME_RECORD_TURF_WINS].parse::<u32>().ok(),
            lifetime_record_turf_places: line[SF_LIFETIME_RECORD_TURF_PLACES].parse::<u32>().ok(),
            lifetime_record_turf_shows: line[SF_LIFETIME_RECORD_TURF_SHOWS].parse::<u32>().ok(),
            lifetime_record_turf_earnings: line[SF_LIFETIME_RECORD_TURF_EARNINGS]
                .parse::<u32>().ok(),
            lifetime_record_wet_starts: line[SF_LIFETIME_RECORD_WET_STARTS].parse::<u32>().ok(),
            lifetime_record_wet_wins: line[SF_LIFETIME_RECORD_WET_WINS].parse::<u32>().ok(),
            lifetime_record_wet_places: line[SF_LIFETIME_RECORD_WET_PLACES].parse::<u32>().ok(),
            lifetime_record_wet_shows: line[SF_LIFETIME_RECORD_WET_SHOWS].parse::<u32>().ok(),
            lifetime_record_wet_earnings: line[SF_LIFETIME_RECORD_WET_EARNINGS].parse::<u32>().ok(),
            current_year_record_year: line[SF_CURRENT_YEAR_RECORD_YEAR].parse::<u32>().ok(),
            current_year_record_starts: line[SF_CURRENT_YEAR_RECORD_STARTS].parse::<u32>().ok(),
            current_year_record_wins: line[SF_CURRENT_YEAR_RECORD_WINS].parse::<u32>().ok(),
            current_year_record_places: line[SF_CURRENT_YEAR_RECORD_PLACES].parse::<u32>().ok(),
            current_year_record_shows: line[SF_CURRENT_YEAR_RECORD_SHOWS].parse::<u32>().ok(),
            current_year_record_earnings: line[SF_CURRENT_YEAR_RECORD_EARNINGS].parse::<u32>().ok(),
            previous_year_record_year: line[SF_PREVIOUS_YEAR_RECORD_YEAR].parse::<u32>().ok(),
            previous_year_record_starts: line[SF_PREVIOUS_YEAR_RECORD_STARTS].parse::<u32>().ok(),
            previous_year_record_wins: line[SF_PREVIOUS_YEAR_RECORD_WINS].parse::<u32>().ok(),
            previous_year_record_places: line[SF_PREVIOUS_YEAR_RECORD_PLACES].parse::<u32>().ok(),
            previous_year_record_shows: line[SF_PREVIOUS_YEAR_RECORD_SHOWS].parse::<u32>().ok(),
            previous_year_record_earnings: line[SF_PREVIOUS_YEAR_RECORD_EARNINGS]
                .parse::<u32>().ok(),
            lifetime_record_starts: line[SF_LIFETIME_RECORD_STARTS].parse::<u32>().ok(),
            lifetime_record_wins: line[SF_LIFETIME_RECORD_WINS].parse::<u32>().ok(),
            lifetime_record_places: line[SF_LIFETIME_RECORD_PLACES].parse::<u32>().ok(),
            lifetime_record_shows: line[SF_LIFETIME_RECORD_SHOWS].parse::<u32>().ok(),
            lifetime_record_earnings: line[SF_LIFETIME_RECORD_EARNINGS].parse::<u32>().ok(),
            bris_run_style: line[SF_BRIS_RUN_STYLE].clone(),
            quirin_speed_points: line[SF_QUIRIN_SPEED_POINTS].parse::<u32>().ok(),
            trainer_jockey_combo_starts: line[SF_TRAINER_JOCKEY_COMBO_STARTS].parse::<u32>().ok(),
            trainer_jockey_combo_wins: line[SF_TRAINER_JOCKEY_COMBO_WINS].parse::<u32>().ok(),
            trainer_jockey_combo_places: line[SF_TRAINER_JOCKEY_COMBO_PLACES].parse::<u32>().ok(),
            trainer_jockey_combo_shows: line[SF_TRAINER_JOCKEY_COMBO_SHOWS].parse::<u32>().ok(),
            trainer_jockey_combo_roi: line[SF_TRAINER_JOCKEY_COMBO_ROI].parse::<f64>().ok(),
            days_since_last_race: line[SF_DAYS_SINCE_LAST_RACE].parse::<u32>().ok(),
            lifetime_all_weather_starts: line[SF_LIFETIME_ALL_WEATHER_STARTS].parse::<u32>().ok(),
            lifetime_all_weather_wins: line[SF_LIFETIME_ALL_WEATHER_WINS].parse::<u32>().ok(),
            lifetime_all_weather_places: line[SF_LIFETIME_ALL_WEATHER_PLACES].parse::<u32>().ok(),
            lifetime_all_weather_shows: line[SF_LIFETIME_ALL_WEATHER_SHOWS].parse::<u32>().ok(),
            lifetime_all_weather_earnings: line[SF_LIFETIME_ALL_WEATHER_EARNINGS]
                .parse::<u32>().ok(),
            best_bris_speed_all_weather_surface: line[SF_BEST_BRIS_SPEED_ALL_WEATHER]
                .parse::<u32>().ok(),
            bris_prime_power_rating: line[SF_BRIS_PRIME_POWER_RATING].parse::<f64>().ok(),
            trainer_starts_current_year: line[SF_TRAINER_STARTS_CURRENT_YEAR].parse::<u32>().ok(),
            trainer_wins_current_year: line[SF_TRAINER_WINS_CURRENT_YEAR].parse::<u32>().ok(),
            trainer_places_current_year: line[SF_TRAINER_PLACES_CURRENT_YEAR].parse::<u32>().ok(),
            trainer_shows_current_year: line[SF_TRAINER_SHOWS_CURRENT_YEAR].parse::<u32>().ok(),
            trainer_roi_current_year: line[SF_TRAINER_ROI_CURRENT_YEAR].parse::<f64>().ok(),
            trainer_starts_previous_year: line[SF_TRAINER_STARTS_PREVIOUS_YEAR].parse::<u32>().ok(),
            trainer_wins_previous_year: line[SF_TRAINER_WINS_PREVIOUS_YEAR].parse::<u32>().ok(),
            trainer_places_previous_year: line[SF_TRAINER_PLACES_PREVIOUS_YEAR].parse::<u32>().ok(),
            trainer_shows_previous_year: line[SF_TRAINER_SHOWS_PREVIOUS_YEAR].parse::<u32>().ok(),
            trainer_roi_previous_year: line[SF_TRAINER_ROI_PREVIOUS_YEAR].parse::<f64>().ok(),
            jockey_starts_current_year: line[SF_JOCKEY_STARTS_CURRENT_YEAR].parse::<u32>().ok(),
            jockey_wins_current_year: line[SF_JOCKEY_WINS_CURRENT_YEAR].parse::<u32>().ok(),
            jockey_places_current_year: line[SF_JOCKEY_PLACES_CURRENT_YEAR].parse::<u32>().ok(),
            jockey_shows_current_year: line[SF_JOCKEY_SHOWS_CURRENT_YEAR].parse::<u32>().ok(),
            jockey_roi_current_year: line[SF_JOCKEY_ROI_CURRENT_YEAR].parse::<f64>().ok(),
            jockey_starts_previous_year: line[SF_JOCKEY_STARTS_PREVIOUS_YEAR].parse::<u32>().ok(),
            jockey_wins_previous_year: line[SF_JOCKEY_WINS_PREVIOUS_YEAR].parse::<u32>().ok(),
            jockey_places_previous_year: line[SF_JOCKEY_PLACES_PREVIOUS_YEAR].parse::<u32>().ok(),
            jockey_shows_previous_year: line[SF_JOCKEY_SHOWS_PREVIOUS_YEAR].parse::<u32>().ok(),
            jockey_roi_previous_year: line[SF_JOCKEY_ROI_PREVIOUS_YEAR].parse::<f64>().ok(),
            sire_stud_fee: line[SF_SIRE_STUD_FEE].parse::<u32>().ok(),
            best_bris_speed_fast_track: line[SF_BEST_BRIS_SPEED_FAST_TRACK].parse::<u32>().ok(),
            best_bris_speed_turf: line[SF_BEST_BRIS_SPEED_TURF].parse::<u32>().ok(),
            best_bris_speed_off_track: line[SF_BEST_BRIS_SPEED_OFF_TRACK].parse::<u32>().ok(),
            best_bris_speed_distance: line[SF_BEST_BRIS_SPEED_DISTANCE].parse::<i32>().ok(),
            auction_price: line[SF_AUCTION_PRICE].parse::<u32>().ok(),
            where_when_sold_at_auction: line[SF_WHERE_WHEN_SOLD_AT_AUCTION].clone(),
            bris_dirt_pedigree_rating: line[SF_BRIS_DIRT_PEDIGREE_RATING].clone(),
            bris_mud_pedigree_rating: line[SF_BRIS_MUD_PEDIGREE_RATING].clone(),
            bris_turf_pedigree_rating: line[SF_BRIS_TURF_PEDIGREE_RATING].clone(),
            bris_distance_pedigree_rating: line[SF_BRIS_DISTANCE_PEDIGREE_RATING].clone(),
            best_bris_speed_life: line[SF_BEST_BRIS_SPEED_LIFE].parse::<u32>().ok(),
            best_bris_speed_most_recent_year: line[SF_BEST_BRIS_SPEED_MOST_RECENT_YEAR]
                .parse::<u32>().ok(),
            best_bris_speed_2nd_most_recent_year: line[SF_BEST_BRIS_SPEED_2ND_MOST_RECENT_YEAR]
                .parse::<u32>().ok(),
            best_bris_speed_todays_track: line[SF_BEST_BRIS_SPEED_TODAYS_TRACK].parse::<u32>().ok(),
            starts_fast_dirt: line[SF_STARTS_FAST_DIRT].parse::<u32>().ok(),
            wins_fast_dirt: line[SF_WINS_FAST_DIRT].parse::<u32>().ok(),
            places_fast_dirt: line[SF_PLACES_FAST_DIRT].parse::<u32>().ok(),
            shows_fast_dirt: line[SF_SHOWS_FAST_DIRT].parse::<u32>().ok(),
            earnings_fast_dirt: line[SF_EARNINGS_FAST_DIRT].parse::<u32>().ok(),
            jockey_distance_turf_label: line[SF_JOCKEY_DISTANCE_TURF_LABEL].clone(),
            jockey_distance_turf_starts: line[SF_JOCKEY_DISTANCE_TURF_STARTS].parse::<u32>().ok(),
            jockey_distance_turf_wins: line[SF_JOCKEY_DISTANCE_TURF_WINS].parse::<u32>().ok(),
            jockey_distance_turf_places: line[SF_JOCKEY_DISTANCE_TURF_PLACES].parse::<u32>().ok(),
            jockey_distance_turf_shows: line[SF_JOCKEY_DISTANCE_TURF_SHOWS].parse::<u32>().ok(),
            jockey_distance_turf_roi: line[SF_JOCKEY_DISTANCE_TURF_ROI].parse::<f64>().ok(),
            jockey_distance_turf_earnings: line[SF_JOCKEY_DISTANCE_TURF_EARNINGS]
                .parse::<u32>().ok(),
            trainer_jockey_combo_starts_meet: line[SF_TRAINER_JOCKEY_COMBO_STARTS_MEET]
                .parse::<u32>().ok(),
            trainer_jockey_combo_wins_meet: line[SF_TRAINER_JOCKEY_COMBO_WINS_MEET]
                .parse::<u32>().ok(),
            trainer_jockey_combo_places_meet: line[SF_TRAINER_JOCKEY_COMBO_PLACES_MEET]
                .parse::<u32>().ok(),
            trainer_jockey_combo_shows_meet: line[SF_TRAINER_JOCKEY_COMBO_SHOWS_MEET]
                .parse::<u32>().ok(),
            trainer_jockey_combo_roi_meet: line[SF_TRAINER_JOCKEY_COMBO_ROI_MEET]
                .parse::<f64>().ok(),
            note: "".to_string(),
            workouts: Vec::new(),
            past_performances: Vec::new(),
            key_trainer_stats: Vec::new(),
        };

        for j in 0..12 {
            if &line[SF_WORKOUT_DATE + j] == "" {
                continue;
            }

            let workout = Workout {
                id: 0,
                horse_id: 0,
                date: Transformers::yyyymmdd_to_mmddyyyy(&line[SF_WORKOUT_DATE + j])
                    .unwrap_or_else(|| line[SF_WORKOUT_DATE + j].clone()),
                time: line[SF_WORKOUT_TIME + j].parse::<f64>().ok(),
                track: line[SF_WORKOUT_TRACK + j].clone(),
                distance: line[SF_WORKOUT_DISTANCE + j].parse::<i32>().ok(),
                condition: line[SF_WORKOUT_CONDITION + j].clone(),
                description: line[SF_WORKOUT_DESCRIPTION + j].clone(),
                main_inner_track_indicator: line[SF_WORKOUT_MAIN_INNER_TRACK_INDICATOR + j].clone(),
                workouts_that_day_distance: line[SF_WORKOUT_WORKOUTS_THAT_DAY_DISTANCE + j]
                    .parse::<u32>().ok(),
                rank: line[SF_WORKOUT_RANK + j].parse::<u32>().ok(),
            };

            horse.workouts.push(workout);
        }

        for j in 0..10 {
            if &line[SF_PP_RACE_DATE + j] == "" {
                continue;
            }

            let pp = PastPerformance {
                id: 0,
                horse_id: 0,
                race_date: Transformers::yyyymmdd_to_mmddyyyy(&line[SF_PP_RACE_DATE + j])
                    .unwrap_or_else(|| "".to_string()),
                days_since_last_race: line[SF_PP_NUMBER_OF_DAYS_SINCE_LAST_RACE + j]
                    .parse::<u32>().ok(),
                track_code: line[SF_PP_TRACK_CODE + j].clone(),
                bris_track_code: line[SF_PP_BRIS_TRACK_CODE + j].clone(),
                race_number: line[SF_PP_RACE_NUMBER + j].parse::<u32>().ok(),
                track_condition: line[SF_PP_TRACK_CONDITION + j].clone(),
                distance: line[SF_PP_DISTANCE + j].parse::<i32>().ok(),
                surface: line[SF_PP_SURFACE + j].clone(),
                special_chute_indicator: line[SF_PP_SPECIAL_CHUTE_INDICATOR + j].clone(),
                entrants: line[SF_PP_ENTRANTS + j].parse::<u32>().ok(),
                post_position: line[SF_PP_POST_POSITION + j].parse::<u32>().ok(),
                equipment: line[SF_PP_EQUIPMENT + j].clone(),
                racename: line[SF_PP_RACENAME + j].clone(),
                medication: line[SF_PP_MEDICATION + j].parse::<u32>().ok(),
                trip_comment: line[SF_PP_TRIP_COMMENT + j].clone(),
                winners_name: line[SF_PP_WINNERS_NAME + j].clone(),
                place_name: line[SF_PP_PLACE_NAME + j].clone(),
                show_name: line[SF_PP_SHOW_NAME + j].clone(),
                winners_weight: line[SF_PP_WINNERS_WEIGHT_CARRIED + j].parse::<u32>().ok(),
                place_weight: line[SF_PP_PLACE_WEIGHT_CARRIED + j].parse::<u32>().ok(),
                show_weight: line[SF_PP_SHOW_WEIGHT_CARRIED + j].parse::<u32>().ok(),
                winners_margin: line[SF_PP_WINNERS_MARGIN + j].parse::<f64>().ok(),
                place_margin: line[SF_PP_PLACE_MARGIN + j].parse::<f64>().ok(),
                show_margin: line[SF_PP_SHOW_MARGIN + j].parse::<f64>().ok(),
                alternate_comment_line: line[SF_PP_ALTERNATE_COMMENT_LINE + j].clone(),
                weight: line[SF_PP_WEIGHT + j].parse::<u32>().ok(),
                odds: line[SF_PP_ODDS + j].parse::<f64>().ok(),
                entry: line[SF_PP_ENTRY + j].clone(),
                race_classication: line[SF_PP_RACE_CLASSIFICATION + j].clone(),
                claiming_price: line[SF_PP_CLAIMING_PRICE + j].parse::<u32>().ok(),
                purse: line[SF_PP_PURSE + j].parse::<u32>().ok(),
                start_call_position: line[SF_PP_START_CALL_POSITION + j].clone(),
                first_call_position: line[SF_PP_1ST_CALL_POSITION + j].clone(),
                second_call_position: line[SF_PP_2ND_CALL_POSITION + j].clone(),
                gate_call_position: line[SF_PP_GATE_CALL_POSITION + j].clone(),
                stretch_call_position: line[SF_PP_STRETCH_POSITION + j].clone(),
                finish_position: line[SF_PP_FINISH_POSITION + j].clone(),
                money_position: line[SF_PP_MONEY_POSITION + j].clone(),
                start_call_between_lengths_leader: line
                    [SF_PP_START_CALL_BETWEEN_LENGTHS_LEADER_MARGIN + j]
                    .parse::<f64>().ok(),
                start_call_between_lengths: line[SF_PP_START_CALL_BETWEEN_LENGTHS + j]
                    .parse::<f64>().ok(),
                first_call_between_lengths_leader: line
                    [SF_PP_1ST_CALL_BETWEEN_LENGTHS_LEADER_MARGIN + j]
                    .parse::<f64>().ok(),
                first_call_between_lengths: line[SF_PP_1ST_CALL_BETWEEN_LENGTHS + j]
                    .parse::<f64>().ok(),
                second_call_between_lengths_leader: line
                    [SF_PP_2ND_CALL_BETWEEN_LENGTHS_LEADER_MARGIN + j]
                    .parse::<f64>().ok(),
                second_call_between_lengths: line[SF_PP_2ND_CALL_BETWEEN_LENGTHS + j]
                    .parse::<f64>().ok(),
                bris_race_shape_1st_call: line[SF_PP_BRIS_RACE_SHAPE_1ST_CALL + j]
                    .parse::<u32>().ok(),
                stretch_call_between_lengths_leader: line
                    [SF_PP_STRETCH_BETWEEN_LENGTHS_LEADER_MARGIN + j]
                    .parse::<f64>().ok(),
                stretch_call_between_lengths: line[SF_PP_STRETCH_BETWEEN_LENGTHS + j]
                    .parse::<f64>().ok(),
                finish_between_lengths_leader: line[SF_PP_FINISH_BETWEEN_LENGTHS_LEADER_MARGIN + j]
                    .parse::<f64>().ok(),
                finish_between_lengths: line[SF_PP_FINISH_BETWEEN_LENGTHS + j].parse::<f64>().ok(),
                bris_race_shape_2nd_call: line[SF_PP_BRIS_RACE_SHAPE_2ND_CALL + j]
                    .parse::<u32>().ok(),
                bris_2f_pace: line[SF_PP_BRIS_2F_PACE + j].parse::<u32>().ok(),
                bris_4f_pace: line[SF_PP_BRIS_4F_PACE + j].parse::<u32>().ok(),
                bris_6f_pace: line[SF_PP_BRIS_6F_PACE + j].parse::<u32>().ok(),
                bris_8f_pace: line[SF_PP_BRIS_8F_PACE + j].parse::<u32>().ok(),
                bris_10f_pace: line[SF_PP_BRIS_10F_PACE + j].parse::<u32>().ok(),
                bris_late_pace: line[SF_PP_BRIS_LATE_PACE + j].parse::<u32>().ok(),
                bris_speed_rating: line[SF_PP_BRIS_SPEED_RATING + j].parse::<u32>().ok(),
                speed_rating: line[SF_PP_SPEED_RATING + j].parse::<u32>().ok(),
                track_variant: line[SF_PP_TRACK_VARIANT + j].parse::<i32>().ok(),
                two_f_fraction: line[SF_PP_2F_FRACTION + j].parse::<f64>().ok(),
                three_f_fraction: line[SF_PP_3F_FRACTION + j].parse::<f64>().ok(),
                four_f_fraction: line[SF_PP_4F_FRACTION + j].parse::<f64>().ok(),
                five_f_fraction: line[SF_PP_5F_FRACTION + j].parse::<f64>().ok(),
                six_f_fraction: line[SF_PP_6F_FRACTION + j].parse::<f64>().ok(),
                seven_f_fraction: line[SF_PP_7F_FRACTION + j].parse::<f64>().ok(),
                eight_f_fraction: line[SF_PP_8F_FRACTION + j].parse::<f64>().ok(),
                ten_f_fraction: line[SF_PP_10F_FRACTION + j].parse::<f64>().ok(),
                twelve_f_fraction: line[SF_PP_12F_FRACTION + j].parse::<f64>().ok(),
                fourteen_f_fraction: line[SF_PP_14F_FRACTION + j].parse::<f64>().ok(),
                sixteen_f_fraction: line[SF_PP_16F_FRACTION + j].parse::<f64>().ok(),
                fraction_1: line[SF_PP_FRACTION_1 + j].parse::<f64>().ok(),
                fraction_2: line[SF_PP_FRACTION_2 + j].parse::<f64>().ok(),
                fraction_3: line[SF_PP_FRACTION_3 + j].parse::<f64>().ok(),
                final_time: line[SF_PP_FINAL_TIME + j].parse::<f64>().ok(),
                claimed_code: line[SF_PP_CLAIMED_CODE + j].clone(),
                trainer: line[SF_PP_TRAINER + j].clone(),
                jockey: line[SF_PP_JOCKEY + j].clone(),
                apprentice_weight_allowance: line[SF_PP_APPRENTICE_WEIGHT_ALLOWANCE + j]
                    .parse::<u32>().ok(),
                race_type: line[SF_PP_RACE_TYPE + j].clone(),
                age_sex_restrictions: line[SF_PP_AGE_SEX_RESTRICTIONS + j].clone(),
                statebred_flag: line[SF_PP_STATEBRED_FLAG + j].clone(),
                restricted_qualifier_flag: line[SF_PP_RESTRICTED_QUALIFIER_FLAG + j].clone(),
                favorite_indicator: line[SF_PP_FAVORITE_INDICATOR + j].clone(),
                front_bandages_indicator: line[SF_PP_FRONT_BANDAGES_INDICATOR + j].clone(),
                bris_speed_par_for_race: line[SF_PP_BRIS_SPEED_PAR_FOR_RACE + j]
                    .parse::<u32>().ok(),
                bar_shoes: line[SF_PP_BAR_SHOES + j].clone(),
                company_line_codes: line[SF_PP_COMPANY_LINE_CODES + j].clone(),
                low_claiming_price_of_race: line[SF_PP_LOW_CLAIMING_PRICE_OF_RACE + j]
                    .parse::<u32>().ok(),
                high_claiming_price_of_race: line[SF_PP_HIGH_CLAIMING_PRICE_OF_RACE + j]
                    .parse::<u32>().ok(),
                code_for_prior_races: line[SF_PP_CODE_FOR_PRIOR_RACES + j].clone(),
                claimed_and_trainer_switches_1: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_1 + j]
                    .clone(),
                claimed_and_trainer_switches_2: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_2 + j]
                    .clone(),
                claimed_and_trainer_switches_3: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_3 + j]
                    .clone(),
                claimed_and_trainer_switches_4: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_4 + j]
                    .clone(),
                claimed_and_trainer_switches_5: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_5 + j]
                    .clone(),
                claimed_and_trainer_switches_6: line[SF_PP_CLAIMED_AND_TRAINER_SWITCHES_6 + j]
                    .clone(),
                extended_start_comment: line[SF_PP_EXTENDED_START_COMMENT + j].clone(),
                sealed_track_indicator: line[SF_PP_SEALED_TRACK + j].clone(),
                previous_all_weather_surface_indicator: line
                    [SF_PP_PREVIOUS_ALL_WEATHER_SURFACE_INDICATOR + j]
                    .clone(),
                equibase_abbreviated_race_condition: line
                    [SF_PP_EQUIBASE_ABBREVIATED_RACE_CONDITIONS + j]
                    .clone(),
            };

            horse.past_performances.push(pp);
        }

        for j in 0..6 {
            if &lines[0][SF_KEY_TRAINER_STAT + j * 5] == "" {
                continue;
            }

            let key_trainer_stat = KeyTrainerStat {
                id: 0,
                horse_id: 0,
                category: lines[0][SF_KEY_TRAINER_STAT + j * 5].clone(),
                starts: lines[0][SF_KEY_TRAINER_STAT + 1 + j * 5]
                    .parse::<u32>().ok(),
                win_pct: lines[0][SF_KEY_TRAINER_STAT + 2 + j * 5]
                    .parse::<f64>().ok(),
                in_the_money_pct: lines[0][SF_KEY_TRAINER_STAT + 3 + j * 5]
                    .parse::<f64>().ok(),
                roi: lines[0][SF_KEY_TRAINER_STAT + 4 + j * 5]
                    .parse::<f64>().ok(),
            };

            horse.key_trainer_stats.push(key_trainer_stat);
        }

        races[race_idx].horses.push(horse);
    }

    let racecard = Racecard {
        id: 0,
        zip_file_name: zip_file_name,
        track: track_name.to_string(),
        date: lines[0][SF_RACE_DATE].clone(),
        long_date: Transformers::prepend_weekday(&lines[0][SF_RACE_DATE])
            .unwrap_or_else(|| lines[0][SF_RACE_DATE].clone()),
        races: races,
    };

    fs::remove_file(&path)
        .await
        .map_err(|e| format!("Failed to delete racecard file: {}", e))?;

    Ok(racecard)
}
