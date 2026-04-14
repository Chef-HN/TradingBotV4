-- V003: Add opened_at to grid_levels
-- Records the exact timestamp when a level transitions from pending to open.

ALTER TABLE grid_levels ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_grid_levels_opened_at ON grid_levels(opened_at);
