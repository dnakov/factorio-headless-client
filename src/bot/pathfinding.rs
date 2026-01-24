use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::codec::{MapPosition, TilePosition};
use crate::codec::map_transfer::MapData;
use crate::codec::map_types::check_player_collision;
use crate::lua::prototype::Prototypes;

const SQRT2: f64 = 1.4142135623730951;
// From PathFindAlgorithm::calcDiagonalHeuristic (octile distance).
const OCTILE_DIAG_COEFF: f64 = -0.5857864376269049; // sqrt(2) - 2
// From PathFindAlgorithm::computeTurnPenalty (scaled by 20).
const TURN_PENALTY_SCALE: f64 = 20.0;

#[derive(Debug, Clone, Copy)]
struct Node {
    pos: (i32, i32),
    g: f64,
    f: f64,
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.f == other.f && self.g == other.g && self.pos == other.pos
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap behavior.
        other
            .f
            .partial_cmp(&self.f)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.g.partial_cmp(&self.g).unwrap_or(Ordering::Equal))
    }
}

/// Tile-based pathfinder that uses Factorio tile/entity collision info.
pub struct TilePathfinder<'a> {
    map: &'a MapData,
    tile_index: HashMap<(i32, i32), usize>,
}

impl<'a> TilePathfinder<'a> {
    pub fn new(map: &'a MapData) -> Self {
        let mut tile_index = HashMap::new();
        for (idx, tile) in map.tiles.iter().enumerate() {
            tile_index.insert((tile.x, tile.y), idx);
        }
        Self { map, tile_index }
    }

    pub fn find_path(
        &self,
        start: MapPosition,
        goal: MapPosition,
        max_nodes: usize,
    ) -> Option<Vec<MapPosition>> {
        let start_tile = TilePosition::from(start);
        let goal_tile = TilePosition::from(goal);
        if start_tile == goal_tile {
            return Some(vec![goal]);
        }

        let start_pos = (start_tile.x, start_tile.y);
        let goal_pos = (goal_tile.x, goal_tile.y);

        let mut open = BinaryHeap::new();
        let mut came_from: HashMap<(i32, i32), (i32, i32)> = HashMap::new();
        let mut g_score: HashMap<(i32, i32), f64> = HashMap::new();

        g_score.insert(start_pos, 0.0);
        open.push(Node {
            pos: start_pos,
            g: 0.0,
            f: octile_heuristic(start_pos, goal_pos),
        });

        let mut expanded = 0usize;
        while let Some(current) = open.pop() {
            if current.pos == goal_pos {
                return Some(self.reconstruct_path(start_pos, goal_pos, goal, came_from));
            }

            let best_g = match g_score.get(&current.pos) {
                Some(v) => *v,
                None => continue,
            };
            if current.g > best_g {
                continue;
            }

            expanded += 1;
            if expanded > max_nodes {
                return None;
            }

            for (next, step_cost) in neighbors(current.pos) {
                let dx = next.0 - current.pos.0;
                let dy = next.1 - current.pos.1;
                if dx != 0 && dy != 0 {
                    // Prevent cutting across blocked corners.
                    if !self.is_walkable(current.pos.0 + dx, current.pos.1)
                        || !self.is_walkable(current.pos.0, current.pos.1 + dy)
                    {
                        continue;
                    }
                }
                if !self.is_walkable(next.0, next.1) {
                    continue;
                }

                let turn_penalty = if let Some(prev) = came_from.get(&current.pos) {
                    compute_turn_penalty(*prev, current.pos, next)
                } else {
                    0.0
                };

                let speed = self.tile_speed(next.0, next.1);
                let step = step_cost / speed + turn_penalty;
                let tentative_g = current.g + step;

                let is_better = match g_score.get(&next) {
                    Some(score) => tentative_g < *score,
                    None => true,
                };
                if is_better {
                    came_from.insert(next, current.pos);
                    g_score.insert(next, tentative_g);
                    let f = tentative_g + octile_heuristic(next, goal_pos);
                    open.push(Node { pos: next, g: tentative_g, f });
                }
            }
        }

        None
    }

    fn reconstruct_path(
        &self,
        start: (i32, i32),
        goal: (i32, i32),
        exact_goal: MapPosition,
        mut came_from: HashMap<(i32, i32), (i32, i32)>,
    ) -> Vec<MapPosition> {
        let mut tiles = Vec::new();
        let mut current = goal;
        tiles.push(current);
        while current != start {
            if let Some(prev) = came_from.remove(&current) {
                current = prev;
                tiles.push(current);
            } else {
                break;
            }
        }
        tiles.reverse();

        if tiles.first().copied() == Some(start) {
            tiles.remove(0);
        }

        if tiles.is_empty() {
            return vec![exact_goal];
        }

        let mut path: Vec<MapPosition> = tiles
            .into_iter()
            .map(|(x, y)| MapPosition::from_tiles(x as f64 + 0.5, y as f64 + 0.5))
            .collect();
        if let Some(last) = path.last_mut() {
            *last = exact_goal;
        }
        path
    }

    fn is_walkable(&self, x: i32, y: i32) -> bool {
        if let Some(tile_name) = self.tile_name(x, y) {
            if !tile_walkable(tile_name) {
                return false;
            }
        }

        let px = x as f64 + 0.5;
        let py = y as f64 + 0.5;
        !check_player_collision(&self.map.entities, px, py)
    }

    fn tile_speed(&self, x: i32, y: i32) -> f64 {
        let Some(name) = self.tile_name(x, y) else {
            return 1.0;
        };
        Prototypes::global()
            .and_then(|p| p.tile(name))
            .map(|t| t.walking_speed_modifier.max(0.05))
            .unwrap_or(1.0)
    }

    fn tile_name(&self, x: i32, y: i32) -> Option<&str> {
        self.tile_index
            .get(&(x, y))
            .and_then(|idx| self.map.tiles.get(*idx))
            .map(|t| t.name.as_str())
    }
}

fn tile_walkable(name: &str) -> bool {
    if let Some(protos) = Prototypes::global() {
        if let Some(tile) = protos.tile(name) {
            if let Some(mask) = &tile.collision_mask {
                return !mask.iter().any(|layer| layer == "player" || layer == "out_of_map");
            }
        }
    }

    let n = name.to_ascii_lowercase();
    if n.contains("out-of-map") || n.contains("out_of_map") {
        return false;
    }
    if n.contains("water") || n.contains("deepwater") || n.contains("lava") || n.contains("ocean") {
        return false;
    }
    true
}

fn octile_heuristic(a: (i32, i32), b: (i32, i32)) -> f64 {
    let dx = (a.0 - b.0).abs() as f64;
    let dy = (a.1 - b.1).abs() as f64;
    let min = dx.min(dy);
    (dx + dy) + OCTILE_DIAG_COEFF * min
}

fn compute_turn_penalty(prev: (i32, i32), curr: (i32, i32), next: (i32, i32)) -> f64 {
    let a1 = angle_fraction(curr.0 - prev.0, curr.1 - prev.1);
    let a2 = angle_fraction(next.0 - curr.0, next.1 - curr.1);
    let diff = (a1 - a2).abs();
    let diff = diff.min(1.0 - diff);
    diff * TURN_PENALTY_SCALE
}

fn angle_fraction(dx: i32, dy: i32) -> f64 {
    if dx == 0 && dy == 0 {
        return 0.0;
    }
    let angle = (dy as f64).atan2(dx as f64);
    let mut frac = angle * (1.0 / (2.0 * std::f64::consts::PI));
    frac = frac.fract();
    if frac < 0.0 {
        frac += 1.0;
    }
    frac
}

fn neighbors(pos: (i32, i32)) -> [( (i32, i32), f64 ); 8] {
    let (x, y) = pos;
    [
        ((x, y - 1), 1.0),
        ((x + 1, y - 1), SQRT2),
        ((x + 1, y), 1.0),
        ((x + 1, y + 1), SQRT2),
        ((x, y + 1), 1.0),
        ((x - 1, y + 1), SQRT2),
        ((x - 1, y), 1.0),
        ((x - 1, y - 1), SQRT2),
    ]
}
