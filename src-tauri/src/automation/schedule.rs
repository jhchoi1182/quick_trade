//! KST 벽시계 기반 자동매매 정규 슬롯과 장 마감 경계.

use chrono::{NaiveDateTime, NaiveTime, Timelike};

use crate::util::naive_to_fake_epoch;

pub const FIRST_DECISION_HOUR: u32 = 9;
pub const FIRST_DECISION_MINUTE: u32 = 5;
pub const LAST_DECISION_HOUR: u32 = 15;
pub const LAST_DECISION_MINUTE: u32 = 5;
pub const FINAL_EXPIRY_HOUR: u32 = 15;
pub const FINAL_EXPIRY_MINUTE: u32 = 10;
pub const FLATTEN_HOUR: u32 = 15;
pub const FLATTEN_MINUTE: u32 = 15;

fn at(date_time: NaiveDateTime, hour: u32, minute: u32) -> NaiveDateTime {
    date_time
        .date()
        .and_time(NaiveTime::from_hms_opt(hour, minute, 0).unwrap())
}

/// 현재 시각 이후의 다음 5분 정규 슬롯. 경계 정각도 다음 슬롯이 아니라 현재 슬롯이다.
pub fn next_decision_slot(now_fake_epoch: i64) -> Option<i64> {
    let now = chrono::DateTime::from_timestamp(now_fake_epoch, 0)?.naive_utc();
    let first = at(now, FIRST_DECISION_HOUR, FIRST_DECISION_MINUTE);
    let last = at(now, LAST_DECISION_HOUR, LAST_DECISION_MINUTE);
    if now <= first {
        return Some(naive_to_fake_epoch(first));
    }
    if now > last {
        return None;
    }

    let minute_of_day = now.hour() * 60 + now.minute();
    let first_minute = FIRST_DECISION_HOUR * 60 + FIRST_DECISION_MINUTE;
    let offset = minute_of_day - first_minute;
    let add_minutes = if offset % 5 == 0 && now.second() == 0 {
        0
    } else {
        5 - (offset % 5)
    };
    let slot = now
        .date()
        .and_time(NaiveTime::from_hms_opt(now.hour(), now.minute(), 0).unwrap())
        + chrono::Duration::minutes(i64::from(add_minutes));
    (slot <= last).then(|| naive_to_fake_epoch(slot))
}

pub fn following_slot(slot_fake_epoch: i64) -> Option<i64> {
    let slot = chrono::DateTime::from_timestamp(slot_fake_epoch, 0)?.naive_utc();
    let next = slot + chrono::Duration::minutes(5);
    (next <= at(slot, LAST_DECISION_HOUR, LAST_DECISION_MINUTE)).then(|| naive_to_fake_epoch(next))
}

/// 절전·날짜 변경 뒤 오래된 슬롯을 재생하지 않고 현재 거래일의 정규 슬롯로 복구한다.
/// 현재 슬롯의 5분 유효구간 안이면 그대로 유지하여 막 깨어난 호출은 허용한다.
pub fn recover_decision_slot(current: Option<i64>, now_fake_epoch: i64) -> Option<i64> {
    let now = chrono::DateTime::from_timestamp(now_fake_epoch, 0)?.naive_utc();
    if let Some(slot_epoch) = current {
        let slot = chrono::DateTime::from_timestamp(slot_epoch, 0)?.naive_utc();
        if slot.date() == now.date() && now_fake_epoch < slot_epoch.saturating_add(300) {
            return Some(slot_epoch);
        }
    }
    next_decision_slot(now_fake_epoch)
}

/// 내구화된 다음 슬롯과 마지막 호출 슬롯을 함께 복원한다. 정확히 5분 경계에서
/// 프로세스가 재시작되거나 15:05 호출 직후 종료돼도 같은 슬롯을 다시 반환하지 않는다.
pub fn recover_persisted_decision_slot(
    current: Option<i64>,
    last_decision_slot: Option<i64>,
    now_fake_epoch: i64,
) -> Option<i64> {
    let recovered = recover_decision_slot(current, now_fake_epoch);
    let Some(last) = last_decision_slot else {
        return recovered;
    };
    let Some(now) =
        chrono::DateTime::from_timestamp(now_fake_epoch, 0).map(|value| value.naive_utc())
    else {
        return recovered;
    };
    let Some(last_time) = chrono::DateTime::from_timestamp(last, 0).map(|value| value.naive_utc())
    else {
        return recovered;
    };
    if last_time.date() != now.date() || recovered.is_some_and(|slot| slot > last) {
        return recovered;
    }
    following_slot(last)
}

pub fn scenario_expiry(slot_fake_epoch: i64) -> Option<i64> {
    let slot = chrono::DateTime::from_timestamp(slot_fake_epoch, 0)?.naive_utc();
    let expiry =
        (slot + chrono::Duration::minutes(5)).min(at(slot, FINAL_EXPIRY_HOUR, FINAL_EXPIRY_MINUTE));
    Some(naive_to_fake_epoch(expiry))
}

pub fn flatten_at(now_fake_epoch: i64) -> Option<i64> {
    let now = chrono::DateTime::from_timestamp(now_fake_epoch, 0)?.naive_utc();
    Some(naive_to_fake_epoch(at(now, FLATTEN_HOUR, FLATTEN_MINUTE)))
}

pub fn is_at_or_after_flatten(now_fake_epoch: i64) -> bool {
    flatten_at(now_fake_epoch).is_some_and(|flatten| now_fake_epoch >= flatten)
}

/// 포지션의 첫 체결 거래일 15:15가 지났는지 판정한다.
///
/// 단순히 `now`의 시각만 보면 다음 거래일 오전에는 15:15 이전으로 되돌아가 전일
/// 포지션을 최대보유 청산으로 오분류한다. 앱 종료·절전 뒤에도 장마감 우선순위를
/// 유지하려면 첫 체결일의 경계와 비교해야 한다.
pub fn has_reached_position_flatten(first_fill_at: i64, now_fake_epoch: i64) -> bool {
    flatten_at(first_fill_at).is_some_and(|flatten| now_fake_epoch >= flatten)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn epoch(value: &str) -> i64 {
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp()
    }

    #[test]
    fn regular_slots_and_last_expiry() {
        assert_eq!(
            next_decision_slot(epoch("2026-07-22 09:04:59")),
            Some(epoch("2026-07-22 09:05:00"))
        );
        assert_eq!(
            next_decision_slot(epoch("2026-07-22 10:20:00")),
            Some(epoch("2026-07-22 10:20:00"))
        );
        assert_eq!(
            next_decision_slot(epoch("2026-07-22 10:20:01")),
            Some(epoch("2026-07-22 10:25:00"))
        );
        assert_eq!(
            scenario_expiry(epoch("2026-07-22 15:05:00")),
            Some(epoch("2026-07-22 15:10:00"))
        );
        assert_eq!(following_slot(epoch("2026-07-22 15:05:00")), None);
    }

    #[test]
    fn after_market_has_no_decision_slot() {
        assert_eq!(next_decision_slot(epoch("2026-07-22 15:05:01")), None);
        assert!(is_at_or_after_flatten(epoch("2026-07-22 15:15:00")));
    }

    #[test]
    fn 다음날_오전에도_첫체결일_장마감은_지난상태다() {
        let first_fill = epoch("2026-07-22 14:59:00");
        assert!(!has_reached_position_flatten(
            first_fill,
            epoch("2026-07-22 15:14:59")
        ));
        assert!(has_reached_position_flatten(
            first_fill,
            epoch("2026-07-22 15:15:00")
        ));
        assert!(has_reached_position_flatten(
            first_fill,
            epoch("2026-07-23 09:00:00")
        ));
    }

    #[test]
    fn 마지막_슬롯_뒤에도_다음_거래일_스케줄을_복구한다() {
        assert_eq!(
            recover_decision_slot(None, epoch("2026-07-23 08:30:00")),
            Some(epoch("2026-07-23 09:05:00"))
        );
        assert_eq!(
            recover_decision_slot(
                Some(epoch("2026-07-22 10:20:00")),
                epoch("2026-07-23 10:21:00")
            ),
            Some(epoch("2026-07-23 10:25:00"))
        );
        assert_eq!(
            recover_decision_slot(
                Some(epoch("2026-07-23 10:20:00")),
                epoch("2026-07-23 10:22:00")
            ),
            Some(epoch("2026-07-23 10:20:00"))
        );
    }

    #[test]
    fn 정확한_경계_재시작에서도_이미_호출한_슬롯을_반복하지_않는다() {
        let ten_twenty = epoch("2026-07-23 10:20:00");
        assert_eq!(
            recover_persisted_decision_slot(None, Some(ten_twenty), ten_twenty),
            Some(epoch("2026-07-23 10:25:00"))
        );

        let last = epoch("2026-07-23 15:05:00");
        assert_eq!(
            recover_persisted_decision_slot(None, Some(last), last),
            None
        );
    }
}
