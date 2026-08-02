"""Add Claude model mapping assistant tables."""

import sqlalchemy as sa
from alembic import op


revision = "0003_claude_model_mappings"
down_revision = "0002_multi_protocol_channels"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("claude_model_mappings"):
        op.create_table(
            "claude_model_mappings",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("claude_model_id", sa.String(), nullable=False),
            sa.Column("display_name", sa.String(), nullable=True),
            sa.Column("upstream_protocol", sa.String(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("claude_model_id"),
        )
    if not inspector.has_table("claude_mapping_candidates"):
        op.create_table(
            "claude_mapping_candidates",
            sa.Column("id", sa.String(), nullable=False),
            sa.Column("mapping_id", sa.String(), nullable=False),
            sa.Column("channel_model_id", sa.String(), nullable=False),
            sa.Column("priority", sa.Integer(), nullable=False),
            sa.Column("enabled", sa.Boolean(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.ForeignKeyConstraint(
                ["channel_model_id"], ["channel_models.id"], ondelete="RESTRICT"
            ),
            sa.ForeignKeyConstraint(
                ["mapping_id"], ["claude_model_mappings.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("mapping_id", "channel_model_id"),
            sa.UniqueConstraint("mapping_id", "priority"),
        )

    # Add upstream request metadata columns to request_attempts (nullable backfill).
    columns = {column["name"] for column in inspector.get_columns("request_attempts")}
    if "upstream_protocol" not in columns:
        op.add_column(
            "request_attempts", sa.Column("upstream_protocol", sa.String(), nullable=True)
        )
    if "upstream_model_id" not in columns:
        op.add_column(
            "request_attempts", sa.Column("upstream_model_id", sa.String(), nullable=True)
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    columns = {column["name"] for column in inspector.get_columns("request_attempts")}
    if "upstream_model_id" in columns:
        op.drop_column("request_attempts", "upstream_model_id")
    if "upstream_protocol" in columns:
        op.drop_column("request_attempts", "upstream_protocol")
    if inspector.has_table("claude_mapping_candidates"):
        op.drop_table("claude_mapping_candidates")
    if inspector.has_table("claude_model_mappings"):
        op.drop_table("claude_model_mappings")
