use image::codecs::png::PngEncoder;
use image::{ColorType, ImageEncoder, Rgb, RgbImage};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::types::Candle;

pub const COMPOSITE_WIDTH: u32 = 1_280;
pub const COMPOSITE_HEIGHT: u32 = 768;
const VISIBLE_BARS: usize = 60;
const MA_PERIODS: [usize; 4] = [5, 20, 60, 120];

const BG: Rgb<u8> = Rgb([12, 17, 27]);
const PANEL_BG: Rgb<u8> = Rgb([17, 24, 38]);
const GRID: Rgb<u8> = Rgb([42, 51, 68]);
const BORDER: Rgb<u8> = Rgb([72, 82, 102]);
const UP: Rgb<u8> = Rgb([235, 73, 88]);
const DOWN: Rgb<u8> = Rgb([57, 128, 232]);
const FLAT: Rgb<u8> = Rgb([174, 181, 194]);
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
pub struct TimeframeIndicatorPayload {
    pub interval_minutes: u32,
    pub bar_count: usize,
    pub current_ohlcv: Option<Candle>,
    pub moving_averages: MovingAveragePayload,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndicatorPayload {
    pub ten_minute: TimeframeIndicatorPayload,
    pub fifteen_minute: TimeframeIndicatorPayload,
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

/// LLM의 텍스트 입력에 넣을 10·15분 현재 OHLCV와 MA 5/20/60/120.
pub fn indicator_payload(one_minute: &[Candle]) -> IndicatorPayload {
    let ten = aggregate_candles(one_minute, 10);
    let fifteen = aggregate_candles(one_minute, 15);
    IndicatorPayload {
        ten_minute: timeframe_payload(10, &ten),
        fifteen_minute: timeframe_payload(15, &fifteen),
    }
}

/// 최근 60개 10분봉과 15분봉을 좌우 패널로 그린 1280×768 PNG.
/// 외부 폰트 없이 작은 비트맵 라벨, 캔들, 거래량, 4개 이동평균만 그린다.
pub fn render_composite_png(one_minute: &[Candle]) -> AppResult<Vec<u8>> {
    let ten = aggregate_candles(one_minute, 10);
    let fifteen = aggregate_candles(one_minute, 15);
    let mut image = RgbImage::from_pixel(COMPOSITE_WIDTH, COMPOSITE_HEIGHT, BG);

    let margin = 16;
    let gap = 16;
    let panel_width = (COMPOSITE_WIDTH - margin * 2 - gap) / 2;
    let panel_height = COMPOSITE_HEIGHT - margin * 2;
    render_panel(
        &mut image,
        Rect::new(margin, margin, panel_width, panel_height),
        &ten,
        "10 MIN",
        Rgb([79, 145, 255]),
    );
    render_panel(
        &mut image,
        Rect::new(
            margin + panel_width + gap,
            margin,
            panel_width,
            panel_height,
        ),
        &fifteen,
        "15 MIN",
        Rgb([255, 151, 71]),
    );

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

fn timeframe_payload(interval_minutes: u32, bars: &[Candle]) -> TimeframeIndicatorPayload {
    TimeframeIndicatorPayload {
        interval_minutes,
        bar_count: bars.len(),
        current_ohlcv: bars.last().copied(),
        moving_averages: MovingAveragePayload {
            ma5: last_moving_average(bars, 5),
            ma20: last_moving_average(bars, 20),
            ma60: last_moving_average(bars, 60),
            ma120: last_moving_average(bars, 120),
        },
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
    all_bars: &[Candle],
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

    // 이동평균 색 범례. 라벨 없이도 네 선의 색 순서를 고정한다.
    for (index, color) in MA_COLORS.iter().enumerate() {
        let x = panel.right().saturating_sub(116) + index as u32 * 27;
        hline(image, x, x + 18, panel.y + 21, *color);
    }

    let plot_left = panel.x + 14;
    let plot_right = panel.right() - 14;
    let price_top = panel.y + 42;
    let price_bottom = panel.y + panel.height * 72 / 100;
    let volume_top = price_bottom + 15;
    let volume_bottom = panel.bottom() - 14;

    for row in 0..=4 {
        let y = price_top + (price_bottom - price_top) * row / 4;
        hline(image, plot_left, plot_right, y, GRID);
    }
    for column in 0..=6 {
        let x = plot_left + (plot_right - plot_left) * column / 6;
        vline(image, x, price_top, volume_bottom, GRID);
    }
    hline(image, plot_left, plot_right, price_bottom, BORDER);
    hline(image, plot_left, plot_right, volume_top, GRID);

    let start = all_bars.len().saturating_sub(VISIBLE_BARS);
    let bars = &all_bars[start..];
    if bars.is_empty() {
        return;
    }

    let ma_series: Vec<Vec<Option<f64>>> = MA_PERIODS
        .iter()
        .map(|period| moving_average_series(all_bars, *period))
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
        'M' => [
            0b10001, 0b11011, 0b10101, 0b10101, 0b10001, 0b10001, 0b10001,
        ],
        'I' => [
            0b11111, 0b00100, 0b00100, 0b00100, 0b00100, 0b00100, 0b11111,
        ],
        'N' => [
            0b10001, 0b11001, 0b11001, 0b10101, 0b10011, 0b10011, 0b10001,
        ],
        _ => [0; 7],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minute_bars(count: usize) -> Vec<Candle> {
        let base = 3_600_i64;
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
    fn aggregates_ohlcv_into_ten_and_fifteen_minutes() {
        let source = minute_bars(30);
        let ten = aggregate_candles(&source, 10);
        let fifteen = aggregate_candles(&source, 15);

        assert_eq!(ten.len(), 3);
        assert_eq!(fifteen.len(), 2);
        assert_eq!(ten[0].open, 100.0);
        assert_eq!(ten[0].high, 111.0);
        assert_eq!(ten[0].low, 99.0);
        assert_eq!(ten[0].close, 110.0);
        assert_eq!(ten[0].volume, (1..=10).sum::<u32>() as f64);
        assert_eq!(fifteen[1].open, 115.0);
        assert_eq!(fifteen[1].close, 130.0);
    }

    #[test]
    fn payload_contains_current_ohlcv_and_all_moving_averages() {
        let source = minute_bars(1_800);
        let payload = indicator_payload(&source);

        assert_eq!(payload.ten_minute.bar_count, 180);
        assert_eq!(payload.fifteen_minute.bar_count, 120);
        assert!(payload.ten_minute.current_ohlcv.is_some());
        assert!(payload.fifteen_minute.current_ohlcv.is_some());
        assert!(payload.ten_minute.moving_averages.ma120.is_some());
        assert!(payload.fifteen_minute.moving_averages.ma120.is_some());

        let fifteen = aggregate_candles(&source, 15);
        assert_eq!(
            payload.fifteen_minute.moving_averages.ma5,
            last_moving_average(&fifteen, 5)
        );
    }

    #[test]
    fn composite_is_deterministic_1280_by_768_png() {
        let source = minute_bars(1_800);
        let first = render_composite_png(&source).unwrap();
        let second = render_composite_png(&source).unwrap();

        assert_eq!(&first[..8], b"\x89PNG\r\n\x1a\n");
        assert_eq!(first, second);
        let decoded = image::load_from_memory(&first).unwrap();
        assert_eq!(decoded.width(), COMPOSITE_WIDTH);
        assert_eq!(decoded.height(), COMPOSITE_HEIGHT);
    }
}
