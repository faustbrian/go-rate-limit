package postgres

import migrations "github.com/faustbrian/golib/pkg/migrations"

// Migration is the portable SQL definition owned by this package.
type Migration struct {
	// Version is the migrations version.
	Version uint
	// Name is the stable migration name.
	Name string
	// Up creates indexed rate-limit state storage.
	Up string
	// Down removes package-owned state storage.
	Down string
}

// SchemaMigration returns the package-owned PostgreSQL schema migration.
func SchemaMigration() Migration {
	return Migration{
		Version: 1,
		Name:    "create_rate_limit_states",
		Up: `CREATE TABLE rate_limit_states (
    state_key bytea PRIMARY KEY,
    state jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE INDEX rate_limit_states_expires_at_idx
    ON rate_limit_states (expires_at);`,
		Down: "DROP TABLE rate_limit_states;",
	}
}

// GoMigration adapts SchemaMigration to migrations.
func GoMigration() (migrations.Migration, error) {
	migration := SchemaMigration()
	return migrations.NewMigration(
		migrations.Version(migration.Version),
		migration.Name,
		migrations.TransactionModeDefault,
		migration.Up,
		migration.Down,
	)
}
