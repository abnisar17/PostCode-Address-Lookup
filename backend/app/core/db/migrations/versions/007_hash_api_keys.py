"""Hash API keys at rest: add key_hash + key_prefix, relax legacy key column.

Revision ID: 007
Revises: 006
Create Date: 2026-06-29 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_hash VARCHAR(64)")
    op.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS key_prefix VARCHAR(16)")
    # Backfill existing plaintext keys into the hashed form.
    op.execute(
        "UPDATE api_keys SET key_hash = encode(digest(key, 'sha256'), 'hex'), "
        "key_prefix = left(key, 12) WHERE key_hash IS NULL AND key IS NOT NULL"
    )
    op.execute("ALTER TABLE api_keys ALTER COLUMN key DROP NOT NULL")
    op.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_api_keys_key_hash")
    op.execute("ALTER TABLE api_keys DROP COLUMN IF EXISTS key_prefix")
    op.execute("ALTER TABLE api_keys DROP COLUMN IF EXISTS key_hash")
