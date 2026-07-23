use image::codecs::png::PngEncoder;
use image::{ColorType, ImageEncoder, Rgb, RgbImage};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::types::Candle;

pub const COMPOSITE_WIDTH: u32 = 1_600;
pub const COMPOSITE_HEIGHT: u32 = 1_000;
const VISIBLE_BARS: usize = 60;
const PAYLOAD_BARS: usize = 30;
const MA_PERIODS: [usize; 4] = [5, 20, 60, 120];

const BG: Rgb<u8> = Rgb([12, 17, 27]);
const PANEL_BG: Rgb<u8> = Rgb([17, 24, 38]);
const GRID: Rgb<u8> = Rgb([42, 51, 68]);
const BORDER: Rgb<u8> = Rgb([72, 82, 102]);
const UP: Rgb<u8> = Rgb([235, 73, 88]);
const DOWN: Rgb<u8> = Rgb([57, 128, 232]);
const FLAT: Rgb<u8> = Rgb([174, 181, 194]);
const LIVE: Rgb<u8> = Rgb([255, 174, 66]);
const MA_COLORS: [Rgb<u8>; 4] = [
    Rgb([255, 211, 74]),
    Rgb([81, 209, 139]),
    Rgb([211, 112, 247]),
    Rgb([89, 206, 255]),
];

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MovingAveragePayload {
    pub ma5: Option<f64>,
    pub ma20: Option<f64>,
    pub ma60: Option<f64>,
    pub ma120: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AverageVolumePayload {
    pub volume5: Option<f64>,
    pub volume20: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TimeframeIndicatorPayload {
    pub interval_minutes: u32,
    pub completed_candles: Vec<Candle>,
    pub forming_candle: Option<Candle>,
    pub forming_progress_pct: Option<f64>,
    pub moving_averages: MovingAveragePayload,
    pub average_volumes: AverageVolumePayload,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DayIndicatorPayload {
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub hlc3_volume_weighted_average: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorPayload {
    pub as_of_epoch: i64,
    pub day: DayIndicatorPayload,
    pub one_minute: TimeframeIndicatorPayload,
    pub three_minute: TimeframeIndicatorPayload,
    pub five_minute: TimeframeIndicatorPayload,
    pub fifteen_minute: TimeframeIndicatorPayload,
}

#[derive(Debug, Clone)]
struct TimeframeSeries {
    interval_minutes: u32,
    completed: Vec<Candle>,
    forming: Option<Candle>,
    forming_progress_pct: Option<f64>,
}

/// 오름차순 1분봉을 지정 분봉으로 집계한다. 입력 순서가 섞여 있어도
/// 내부에서 정렬하며, 같은 버킷의 시가/고가/저가/종가/거래량을 보존한다.
pub fn aggregate_candles(one_minute: &[Candle], interval_minutes: u32) -> Vec<Candle> {
    if interval_minutes == 0 {
        return Vec::new();
    }

    let mut source: Vec<Candle> = one_minute
        .iter()
        .copied()
        .filter(|bar| {
            bar.open.is_finite()
                && bar.high.is_finite()
                && bar.low.is_finite()
                && bar.close.is_finite()
                && bar.volume.is_finite()
        })
        .collect();
    source.sort_by_key(|bar| bar.time);

    let bucket_seconds = i64::from(interval_minutes) * 60;
    let mut out: Vec<Candle> = Vec::with_capacity(source.len() / interval_minutes as usize + 1);
    for bar in source {
        let bucket = bar.time.div_euclid(bucket_seconds) * bucket_seconds;
        if let Some(last) = out.last_mut().filter(|last| last.time == bucket) {
            last.high = last.high.max(bar.high);
            last.low = last.low.min(bar.low);
            last.close = bar.close;
            last.volume += bar.volume.max(0.0);
        } else {
            out.push(Candle {
                time: bucket,
                open: bar.open,
                high: bar.high,
                low: bar.low,
                close: bar.close,
                volume: bar.volume.max(0.0),
            });
        }
    }
    out
}

pub fn last_moving_average(bars: &[Candle], period: usize) -> Option<f64> {
    if period == 0 || bars.len() < period {
        return None;
    }
    let sum: f64 = bars[bars.len() - period..]
        .iter()
        .map(|bar| bar.close)
        .sum();
    Some(sum / period as f64)
}

fn last_average_volume(bars: &[Candle], period: usize) -> Option<f64> {
    if period == 0 || bars.len() < period {
        return None;
    }
    let sum: f64 = bars[bars.len() - period..]
        .iter()
        .map(|bar| bar.volume.max(0.0))
        .sum();
    Some(sum / period as f64)
}

/// 판단 시각을 기준으로 1·3·5·15분봉의 완성봉과 형성 중 봉을 분리한다.
/// `as_of_epoch`은 Candle.time과 동일한 KST 벽시계 기반 가짜 epoch다.
pub fn indicator_payload(one_minute: &[Candle], as_of_epoch: i64) -> IndicatorPayload {
    IndicatorPayload {
        as_of_epoch,
        day: day_payload(one_minute, as_of_epoch),
        one_minute: timeframe_payload(&timeframe_series(one_minute, 1, as_of_epoch)),
        three_minute: timeframe_payload(&timeframe_series(one_minute, 3, as_of_epoch)),
        five_minute: timeframe_payload(&timeframe_series(one_minute, 5, as_of_epoch)),
        fifteen_minute: timeframe_payload(&timeframe_series(one_minute, 15, as_of_epoch)),
    }
}

/// 최근 60개 1·3·5·15분봉을 2×2 패널로 그린 1600×1000 PNG.
/// 가격·시간축과 거래량, 완성봉 기준 이동평균, 형성 중 봉의 LIVE 표시를 포함한다.
pub fn render_composite_png(one_minute: &[Candle], as_of_epoch: i64) -> AppResult<Vec<u8>> {
    let frames = [
        (
            timeframe_series(one_minute, 1, as_of_epoch),
            "1 MIN",
            Rgb([79, 145, 255]),
        ),
        (
            timeframe_series(one_minute, 3, as_of_epoch),
            "3 MIN",
            Rgb([130, 116, 255]),
        ),
        (
            timeframe_series(one_minute, 5, as_of_epoch),
            "5 MIN",
            Rgb([66, 198, 153]),
        ),
        (
            timeframe_series(one_minute, 15, as_of_epoch),
            "15 MIN",
            Rgb([255, 151, 71]),
        ),
    ];
    let mut image = RgbImage::from_pixel(COMPOSITE_WIDTH, COMPOSITE_HEIGHT, BG);

    let margin = 16;
    let gap = 16;
    let panel_width = (COMPOSITE_WIDTH - margin * 2 - gap) / 2;
    let panel_height = (COMPOSITE_HEIGHT - margin * 2 - gap) / 2;
    for (index, (frame, label, accent)) in frames.iter().enumerate() {
        let column = index as u32 % 2;
        let row = index as u32 / 2;
        render_panel(
            &mut image,
            Rect::new(
                margin + column * (panel_width + gap),
                margin + row * (panel_height + gap),
                panel_width,
                panel_height,
            ),
            frame,
            label,
            *accent,
        );
    }

    let mut png = Vec::new();
    PngEncoder::new(&mut png)
        .write_image(
            image.as_raw(),
            COMPOSITE_WIDTH,
            COMPOSITE_HEIGHT,
            ColorType::Rgb8.into(),
        )
        .map_err(|error| AppError::Parse(format!("차트 PNG 인코딩 실패: {error}")))?;
    Ok(png)
}

fn timeframe_series(
    one_minute: &[Candle],
    interval_minutes: u32,
    as_of_epoch: i64,
) -> TimeframeSeries {
    let visible_source: Vec<Candle> = one_minute
        .iter()
        .copied()
        .filter(|bar| bar.time <= as_of_epoch)
        .collect();
    let aggregated = aggregate_candles(&visible_source, interval_minutes);
    let bucket_seconds = i64::from(interval_minutes) * 60;
    let current_bucket = as_of_epoch.div_euclid(bucket_seconds) * bucket_seconds;
    let split_at = aggregated.partition_point(|bar| bar.time < current_bucket);
    let completed = aggregated[..split_at].to_vec();
    let forming = aggregated
        .get(split_at)
        .copied()
        .filter(|bar| bar.time == current_bucket);
    let forming_progress_pct = forming.map(|_| {
        (as_of_epoch.saturating_sub(current_bucket) as f64 / bucket_seconds as f64 * 100.0)
            .clamp(0.0, 100.0)
    });
    TimeframeSeries {
        interval_minutes,
        completed,
        forming,
        forming_progress_pct,
    }
}

fn timeframe_payload(series: &TimeframeSeries) -> TimeframeIndicatorPayload {
    let start = series.completed.len().saturating_sub(PAYLOAD_BARS);
    TimeframeIndicatorPayload {
        interval_minutes: series.interval_minutes,
        completed_candles: series.completed[start..].to_vec(),
        forming_candle: series.forming,
        forming_progress_pct: series.forming_progress_pct,
        moving_averages: MovingAveragePayload {
            ma5: last_moving_average(&series.completed, 5),
            ma20: last_moving_average(&series.completed, 20),
            ma60: last_moving_average(&series.completed, 60),
            ma120: last_moving_average(&series.completed, 120),
        },
        average_volumes: AverageVolumePayload {
            volume5: last_average_volume(&series.completed, 5),
            volume20: last_average_volume(&series.completed, 20),
        },
    }
}

fn day_payload(one_minute: &[Candle], as_of_epoch: i64) -> DayIndicatorPayload {
    let day = as_of_epoch.div_euclid(86_400);
    let mut bars: Vec<Candle> = one_minute
        .iter()
        .copied()
        .filter(|bar| {
            bar.time <= as_of_epoch
                && bar.time.div_euclid(86_400) == day
                && bar.open.is_finite()
                && bar.high.is_finite()
                && bar.low.is_finite()
                && bar.close.is_finite()
                && bar.volume.is_finite()
        })
        .collect();
    bars.sort_by_key(|bar| bar.time);
    let Some(first) = bars.first() else {
        return DayIndicatorPayload {
            open: None,
            high: None,
            low: None,
            hlc3_volume_weighted_average: None,
        };
    };

    let high = bars
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    let low = bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min);
    let total_volume: f64 = bars.iter().map(|bar| bar.volume.max(0.0)).sum();
    let hlc3_volume_weighted_average = (total_volume > 0.0).then(|| {
        bars.iter()
            .map(|bar| ((bar.high + bar.low + bar.close) / 3.0) * bar.volume.max(0.0))
            .sum::<f64>()
            / total_volume
    });
    DayIndicatorPayload {
        open: Some(first.open),
        high: high.is_finite().then_some(high),
        low: low.is_finite().then_some(low),
        hlc3_volume_weighted_average,
    }
}

#[derive(Clone, Copy)]
struct Rect {
    x: u32,
    y: u32,
    width: u32,
    height: u32,
}

impl Rect {
    const fn new(x: u32, y: u32, width: u32, height: u32) -> Self {
        Self {
            x,
            y,
            width,
            height,
        }
    }

    fn right(self) -> u32 {
        self.x + self.width - 1
    }

    fn bottom(self) -> u32 {
        self.y + self.height - 1
    }
}

fn render_panel(
    image: &mut RgbImage,
    panel: Rect,
    frame: &TimeframeSeries,
    label: &str,
    accent: Rgb<u8>,
) {
    fill_rect(image, panel, PANEL_BG);
    draw_rect(image, panel, BORDER);
    fill_rect(
        image,
        Rect::new(panel.x + 1, panel.y + 1, panel.width - 2, 4),
        accent,
    );
    draw_text(image, panel.x + 14, panel.y + 14, label, accent, 2);

    draw_text(image, panel.x + 100, panel.y + 18, "MA", FLAT, 1);
    let legend = [("5", 5), ("20", 20), ("60", 60), ("120", 120)];
    for (index, ((text, _), color)) in legend.iter().zip(MA_COLORS.iter()).enumerate() {
        let x = panel.x + 134 + index as u32 * 76;
        hline(image, x, x + 20, panel.y + 21, *color);
        draw_text(image, x + 25, panel.y + 18, text, *color, 1);
    }
    if let Some(progress) = frame.forming_progress_pct {
        let live_text = format!("LIVE {:.0}%", progress);
        let width = text_width(&live_text, 1);
        draw_text(
            image,
            panel.right().saturating_sub(width + 13),
            panel.y + 18,
            &live_text,
            LIVE,
            1,
        );
    }

    let plot_left = panel.x + 14;
    let plot_right = panel.right() - 76;
    let price_top = panel.y + 42;
    let price_bottom = panel.y + panel.height * 68 / 100;
    let volume_top = price_bottom + 15;
    let volume_bottom = panel.bottom() - 34;

    for row in 0..=4 {
        let y = price_top + (price_bottom - price_top) * row / 4;
        hline(image, plot_left, plot_right, y, GRID);
    }
    for column in 0..=4 {
        let x = plot_left + (plot_right - plot_left) * column / 4;
        vline(image, x, price_top, volume_bottom, GRID);
    }
    hline(image, plot_left, plot_right, price_bottom, BORDER);
    hline(image, plot_left, plot_right, volume_top, GRID);

    let completed_limit = if frame.forming.is_some() {
        VISIBLE_BARS.saturating_sub(1)
    } else {
        VISIBLE_BARS
    };
    let start = frame.completed.len().saturating_sub(completed_limit);
    let completed_bars = &frame.completed[start..];
    let mut bars = completed_bars.to_vec();
    if let Some(forming) = frame.forming {
        bars.push(forming);
    }
    if bars.is_empty() {
        return;
    }

    let ma_series: Vec<Vec<Option<f64>>> = MA_PERIODS
        .iter()
        .map(|period| moving_average_series(&frame.completed, *period))
        .collect();
    let mut low = bars.iter().map(|bar| bar.low).fold(f64::INFINITY, f64::min);
    let mut high = bars
        .iter()
        .map(|bar| bar.high)
        .fold(f64::NEG_INFINITY, f64::max);
    for series in &ma_series {
        for value in series[start..].iter().flatten() {
            low = low.min(*value);
            high = high.max(*value);
        }
    }
    if !low.is_finite() || !high.is_finite() {
        return;
    }
    if (high - low).abs() < f64::EPSILON {
        let pad = high.abs().max(1.0) * 0.005;
        low -= pad;
        high += pad;
    } else {
        let pad = (high - low) * 0.04;
        low -= pad;
        high += pad;
    }

    let plot_width = plot_right - plot_left + 1;
    let slot = plot_width as f64 / bars.len() as f64;
    let body_half = ((slot * 0.28).floor() as u32).clamp(1, 4);
    let max_volume = bars
        .iter()
        .map(|bar| bar.volume.max(0.0))
        .fold(0.0_f64, f64::max);

    for (index, bar) in bars.iter().enumerate() {
        let x = bar_x(plot_left, slot, index);
        let high_y = price_y(bar.high, low, high, price_top, price_bottom);
        let low_y = price_y(bar.low, low, high, price_top, price_bottom);
        let open_y = price_y(bar.open, low, high, price_top, price_bottom);
        let close_y = price_y(bar.close, low, high, price_top, price_bottom);
        let color = if bar.close > bar.open {
            UP
        } else if bar.close < bar.open {
            DOWN
        } else {
            FLAT
        };

        vline(image, x, high_y.min(low_y), high_y.max(low_y), color);
        let body_top = open_y.min(close_y);
        let body_bottom = open_y.max(close_y).max(body_top + 1).min(price_bottom);
        fill_rect(
            image,
            Rect::new(
                x.saturating_sub(body_half).max(plot_left),
                body_top,
                (body_half * 2 + 1).min(plot_right - x.saturating_sub(body_half) + 1),
                body_bottom - body_top + 1,
            ),
            color,
        );
        if frame.forming.is_some() && index + 1 == bars.len() {
            let left = x.saturating_sub(body_half + 2).max(plot_left);
            let right = (x + body_half + 2).min(plot_right);
            draw_rect(
                image,
                Rect::new(
                    left,
                    high_y.min(low_y).saturating_sub(2).max(price_top),
                    right.saturating_sub(left) + 1,
                    high_y
                        .max(low_y)
                        .saturating_add(2)
                        .min(price_bottom)
                        .saturating_sub(high_y.min(low_y).saturating_sub(2).max(price_top))
                        + 1,
                ),
                LIVE,
            );
        }

        if max_volume > 0.0 {
            let available = volume_bottom - volume_top;
            let height = ((bar.volume.max(0.0) / max_volume) * available as f64)
                .round()
                .clamp(1.0, available as f64) as u32;
            fill_rect(
                image,
                Rect::new(
                    x.saturating_sub(body_half).max(plot_left),
                    volume_bottom - height + 1,
                    (body_half * 2 + 1).min(plot_right - x.saturating_sub(body_half) + 1),
                    height,
                ),
                dim(color),
            );
        }
    }

    for (series_index, series) in ma_series.iter().enumerate() {
        let mut previous: Option<(u32, u32)> = None;
        for (visible_index, value) in series[start..].iter().enumerate() {
            let Some(value) = value else {
                previous = None;
                continue;
            };
            let point = (
                bar_x(plot_left, slot, visible_index),
                price_y(*value, low, high, price_top, price_bottom),
            );
            if let Some(from) = previous {
                line(image, from, point, MA_COLORS[series_index]);
            }
            previous = Some(point);
        }
    }

    for row in 0..=4 {
        let ratio = row as f64 / 4.0;
        let value = high - (high - low) * ratio;
        let y = price_top + (price_bottom - price_top) * row / 4;
        draw_text(
            image,
            plot_right + 8,
            y.saturating_sub(3),
            &format_price(value),
            FLAT,
            1,
        );
    }

    let time_indices = [0, bars.len() / 2, bars.len().saturating_sub(1)];
    for (position, index) in time_indices.into_iter().enumerate() {
        let text = format_time(bars[index].time);
        let x = bar_x(plot_left, slot, index);
        let label_x = match position {
            0 => x.saturating_sub(2),
            1 => x.saturating_sub(text_width(&text, 1) / 2),
            _ => x.saturating_sub(text_width(&text, 1)),
        }
        .max(plot_left)
        .min(plot_right.saturating_sub(text_width(&text, 1)));
        draw_text(image, label_x, volume_bottom + 10, &text, FLAT, 1);
    }
}

fn format_price(value: f64) -> String {
    if value.abs() >= 1_000.0 {
        format!("{value:.0}")
    } else if value.abs() >= 100.0 {
        format!("{value:.1}")
    } else {
        format!("{value:.2}")
    }
}

fn format_time(fake_epoch: i64) -> String {
    let seconds = fake_epoch.rem_euclid(86_400);
    let hour = seconds / 3_600;
    let minute = (seconds % 3_600) / 60;
    format!("{hour:02}:{minute:02}")
}

fn moving_average_series(bars: &[Candle], period: usize) -> Vec<Option<f64>> {
    let mut out = vec![None; bars.len()];
    if period == 0 || bars.len() < period {
        return out;
    }
    let mut sum = 0.0;
    for (index, bar) in bars.iter().enumerate() {
        sum += bar.close;
        if index >= period {
            sum -= bars[index - period].close;
        }
        if index + 1 >= period {
            out[index] = Some(sum / period as f64);
        }
    }
    out
}

fn bar_x(left: u32, slot: f64, index: usize) -> u32 {
    left + ((index as f64 + 0.5) * slot).floor() as u32
}

fn price_y(value: f64, low: f64, high: f64, top: u32, bottom: u32) -> u32 {
    let ratio = ((high - value) / (high - low)).clamp(0.0, 1.0);
    top + (ratio * (bottom - top) as f64).round() as u32
}

fn dim(color: Rgb<u8>) -> Rgb<u8> {
    Rgb([
        (u16::from(color[0]) * 2 / 5) as u8,
        (u16::from(color[1]) * 2 / 5) as u8,
        (u16::from(color[2]) * 2 / 5) as u8,
    ])
}

fn put(image: &mut RgbImage, x: i32, y: i32, color: Rgb<u8>) {
    if x >= 0 && y >= 0 && (x as u32) < image.width() && (y as u32) < image.height() {
        image.put_pixel(x as u32, y as u32, color);
    }
}

fn fill_rect(image: &mut RgbImage, rect: Rect, color: Rgb<u8>) {
    let right = rect.x.saturating_add(rect.width).min(image.width());
    let bottom = rect.y.saturating_add(rect.height).min(image.height());
    for y in rect.y..bottom {
        for x in rect.x..right {
            image.put_pixel(x, y, color);
        }
    }
}

fn draw_rect(image: &mut RgbImage, rect: Rect, color: Rgb<u8>) {
    hline(image, rect.x, rect.right(), rect.y, color);
    hline(image, rect.x, rect.right(), rect.bottom(), color);
    vline(image, rect.x, rect.y, rect.bottom(), color);
    vline(image, rect.right(), rect.y, rect.bottom(), color);
}

fn hline(image: &mut RgbImage, from_x: u32, to_x: u32, y: u32, color: Rgb<u8>) {
    if y >= image.height() {
        return;
    }
    for x in from_x..=to_x.min(image.width() - 1) {
        image.put_pixel(x, y, color);
    }
}

fn vline(image: &mut RgbImage, x: u32, from_y: u32, to_y: u32, color: Rgb<u8>) {
    if x >= image.width() {
        return;
    }
    for y in from_y..=to_y.min(image.height() - 1) {
        image.put_pixel(x, y, color);
    }
}

fn line(image: &mut RgbImage, from: (u32, u32), to: (u32, u32), color: Rgb<u8>) {
    let (mut x0, mut y0) = (from.0 as i32, from.1 as i32);
    let (x1, y1) = (to.0 as i32, to.1 as i32);
    let dx = (x1 - x0).abs();
    let sx = if x0 < x1 { 1 } else { -1 };
    let dy = -(y1 - y0).abs();
    let sy = if y0 < y1 { 1 } else { -1 };
    let mut error = dx + dy;
    loop {
        put(image, x0, y0, color);
        if x0 == x1 && y0 == y1 {
            break;
        }
        let twice = error * 2;
        if twice >= dy {
            error += dy;
            x0 += sx;
        }
        if twice <= dx {
            error += dx;
            y0 += sy;
        }
    }
}

fn draw_text(image: &mut RgbImage, x: u32, y: u32, text: &str, color: Rgb<u8>, scale: u32) {
    let mut cursor = x;
    for character in text.chars() {
        let glyph = glyph(character);
        for (row, bits) in glyph.iter().enumerate() {
            for column in 0..5 {
                if bits & (1 << (4 - column)) != 0 {
                    fill_rect(
                        image,
                        Rect::new(
                            cursor + column * scale,
                            y + row as u32 * scale,
                            scale,
                            scale,
                        ),
                        color,
                    );
                }
            }
        }
        cursor += 6 * scale;
    }
}

fn text_width(text: &str, scale: u32) -> u32 {
    text.chars().count() as u32 * 6 * scale
}

fn glyph(character: char) -> [u8; 7] {
    match character {
        '0' => [
            0b01110, 0b10001, 0b10011, 0b10101, 0b11001, 0b10001, 0b01110,
        ],
        '1' => [
            0b00100, 0b01100, 0b00100, 0b00100, 0b00100, 0b00100, 0b01110,
        ],
        '2' => [
            0b01110, 0b10001, 0b00001, 0b00010, 0b00100, 0b01000, 0b11111,
        ],
        '3' => [
            0b11110, 0b00001, 0b00001, 0b01110, 0b00001, 0b00001, 0b11110,
        ],
        '4' => [
            0b00010, 0b00110, 0b01010, 0b10010, 0b11111, 0b00010, 0b00010,
        ],
        '5' => [
            0b11111, 0b10000, 0b10000, 0b11110, 0b00001, 0b00001, 0b11110,
        ],
        '6' => [
            0b01110, 0b10000, 0b10000, 0b11110, 0b10001, 0b10001, 0b01110,
        ],
        '7' => [
            0b11111, 0b00001, 0b00010, 0b00100, 0b01000, 0b01000, 0b01000,
        ],
        '8' => [
            0b01110, 0b10001, 0b10001, 0b01110, 0b10001, 0b10001, 0b01110,
        ],
        '9' => [
            0b01110, 0b10001, 0b10001, 0b01111, 0b00001, 0b00001, 0b01110,
        ],
        'A' => [
            0b01110, 0b10001, 0b10001, 0b11111, 0b10001, 0b10001, 0b10001,
        ],
        'E' => [
            0b11111, 0b10000, 0b10000, 0b11110, 0b10000, 0b10000, 0b11111,
        ],
        'L' => [
            0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b10000, 0b11111,
        ],
        'M' => [
            0b10001, 0b11011, 0b10101, 0b10101, 0b10001, 0b10001, 0b10001,
        ],
        'I' => [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b11111,
        ],
        'N' => [
            0b10001, 0b11001, 0b11001, 0b10101, 0b10011, 0b10011, 0b10001,
        ],
        'V' => [
            0b10001, 0b10001, 0b10001, 0b10001, 0b10001, 0b01010, 0b00100,
        ],
        ':' => [
            0b00000, 0b00100, 0b00100, 0b00000, 0b00100, 0b00100, 0b00000,
        ],
        '.' => [
            0b00000, 0b00000, 0b00000, 0b00000, 0b00000, 0b00110, 0b00110,
        ],
        '-' => [
            0b00000, 0b00000, 0b00000, 0b11111, 0b00000, 0b00000, 0b00000,
        ],
        '%' => [
            0b11001, 0b11010, 0b00100, 0b01000, 0b10110, 0b00110, 0b00000,
        ],
        _ => [0; 7],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minute_bars(count: usize) -> Vec<Candle> {
        let base = 9 * 3_600_i64;
        (0..count)
            .map(|index| {
                let open = 100.0 + index as f64;
                Candle {
                    time: base + index as i64 * 60,
                    open,
                    high: open + 2.0,
                    low: open - 1.0,
                    close: open + 1.0,
                    volume: index as f64 + 1.0,
                }
            })
            .collect()
    }

    #[test]
    fn 일분봉을_1_3_5_15분_ohlcv로_집계한다() {
        let source = minute_bars(30);
        let one = aggregate_candles(&source, 1);
        let three = aggregate_candles(&source, 3);
        let five = aggregate_candles(&source, 5);
        let fifteen = aggregate_candles(&source, 15);

        assert_eq!(one.len(), 30);
        assert_eq!(three.len(), 10);
        assert_eq!(five.len(), 6);
        assert_eq!(fifteen.len(), 2);
        assert_eq!(three[0].open, 100.0);
        assert_eq!(three[0].high, 104.0);
        assert_eq!(three[0].low, 99.0);
        assert_eq!(three[0].close, 103.0);
        assert_eq!(three[0].volume, (1..=3).sum::<u32>() as f64);
        assert_eq!(fifteen[1].open, 115.0);
        assert_eq!(fifteen[1].close, 130.0);
    }

    #[test]
    fn 판단시각을_기준으로_완성봉과_형성봉을_분리한다() {
        let source = minute_bars(16);
        let as_of = 9 * 3_600 + 15 * 60 + 30;
        let payload = indicator_payload(&source, as_of);

        assert_eq!(payload.one_minute.completed_candles.len(), 15);
        assert_eq!(
            payload.one_minute.forming_candle.unwrap().time,
            9 * 3_600 + 15 * 60
        );
        assert_eq!(payload.one_minute.forming_progress_pct, Some(50.0));
        assert_eq!(payload.three_minute.completed_candles.len(), 5);
        assert_eq!(payload.five_minute.completed_candles.len(), 3);
        assert_eq!(payload.fifteen_minute.completed_candles.len(), 1);
        assert!((payload.fifteen_minute.forming_progress_pct.unwrap() - 3.333_333).abs() < 0.001);
    }

    #[test]
    fn payload는_최근_완성봉_30개와_완성봉_통계만_담는다() {
        let mut source = minute_bars(1_802);
        source.last_mut().unwrap().close = 1_000_000.0;
        let as_of = 9 * 3_600 + 1_801 * 60 + 30;
        let payload = indicator_payload(&source, as_of);

        assert_eq!(payload.one_minute.completed_candles.len(), 30);
        assert_eq!(payload.one_minute.completed_candles[29].close, 1_901.0);
        assert_eq!(
            payload.one_minute.forming_candle.unwrap().close,
            1_000_000.0
        );
        assert_eq!(
            payload.one_minute.moving_averages.ma5,
            last_moving_average(&source[..1_801], 5)
        );
        assert_eq!(
            payload.one_minute.average_volumes.volume20,
            last_average_volume(&source[..1_801], 20)
        );
        assert!(payload.fifteen_minute.moving_averages.ma120.is_some());
    }

    #[test]
    fn 당일_ohl과_hlc3_거래량가중평균을_계산한다() {
        let source = vec![
            Candle {
                time: 9 * 3_600,
                open: 100.0,
                high: 110.0,
                low: 90.0,
                close: 105.0,
                volume: 10.0,
            },
            Candle {
                time: 9 * 3_600 + 60,
                open: 105.0,
                high: 120.0,
                low: 100.0,
                close: 115.0,
                volume: 30.0,
            },
        ];
        let payload = indicator_payload(&source, 9 * 3_600 + 90);
        let expected =
            (((110.0 + 90.0 + 105.0) / 3.0) * 10.0 + ((120.0 + 100.0 + 115.0) / 3.0) * 30.0) / 40.0;

        assert_eq!(payload.day.open, Some(100.0));
        assert_eq!(payload.day.high, Some(120.0));
        assert_eq!(payload.day.low, Some(90.0));
        assert!(
            (payload.day.hlc3_volume_weighted_average.unwrap() - expected).abs() < f64::EPSILON
        );
    }

    #[test]
    fn 복합차트는_결정적_1600x1000_png다() {
        let source = minute_bars(1_800);
        let as_of = source.last().unwrap().time + 30;
        let first = render_composite_png(&source, as_of).unwrap();
        let second = render_composite_png(&source, as_of).unwrap();

        assert_eq!(&first[..8], b"\x89PNG\r\n\x1a\n");
        assert_eq!(first, second);
        let decoded = image::load_from_memory(&first).unwrap();
        assert_eq!(decoded.width(), COMPOSITE_WIDTH);
        assert_eq!(decoded.height(), COMPOSITE_HEIGHT);
        let rgb = decoded.to_rgb8();
        let margin = 16;
        let gap = 16;
        let panel_width = (COMPOSITE_WIDTH - margin * 2 - gap) / 2;
        let panel_height = (COMPOSITE_HEIGHT - margin * 2 - gap) / 2;
        assert_eq!(*rgb.get_pixel(margin + 1, margin + 1), Rgb([79, 145, 255]));
        assert_eq!(
            *rgb.get_pixel(margin + panel_width + gap + 1, margin + 1),
            Rgb([130, 116, 255])
        );
        assert_eq!(
            *rgb.get_pixel(margin + 1, margin + panel_height + gap + 1),
            Rgb([66, 198, 153])
        );
        assert_eq!(
            *rgb.get_pixel(
                margin + panel_width + gap + 1,
                margin + panel_height + gap + 1
            ),
            Rgb([255, 151, 71])
        );
    }
}
