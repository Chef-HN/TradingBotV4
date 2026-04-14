-- Users table (auth-kit UserMixin schema)
CREATE TABLE IF NOT EXISTS users (
    id               VARCHAR(36)  PRIMARY KEY,
    email            VARCHAR(255) NOT NULL,
    password_hash    VARCHAR(255) NOT NULL,
    display_name     VARCHAR(100) NOT NULL,
    preferred_locale VARCHAR(5)   NOT NULL DEFAULT 'es',
    created_at       TIMESTAMPTZ  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_users_email ON users (email);

-- OTP codes table (auth-kit OtpMixin schema)
CREATE TABLE IF NOT EXISTS otp_codes (
    id         VARCHAR(36)  PRIMARY KEY,
    email      VARCHAR(255) NOT NULL,
    code       VARCHAR(6)   NOT NULL,
    created_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ  NOT NULL,
    used       BOOLEAN      DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS ix_otp_codes_email ON otp_codes (email);
