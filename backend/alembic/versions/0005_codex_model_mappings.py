"""Codex model mappings expose Codex-standard model names (OpenAI Responses API).

Like Claude mappings, each Codex mapping references an existing system model
and inherits its route candidates, so no separate candidate table is created.
"""

import sqlalchemy as sa
from alembic import op

revision = "0005_codex_model_mappings"
down_revision = "0004_claude_mapping_upstream_model"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("codex_model_mappings"):
        op.create_table(
            "codex_model_mappings",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("codex_model_id", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=True),
            sa.Column("upstream_protocol", sa.String(), nullable=False),
            sa.Column("upstream_model_id", sa.String(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("codex_model_id"),
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("codex_model_mappings"):
        op.drop_table("codex_model_mappings")
