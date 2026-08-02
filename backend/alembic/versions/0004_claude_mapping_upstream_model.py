"""Claude model mappings now reference existing system models directly."""

import sqlalchemy as sa
from alembic import op


revision = "0004_claude_mapping_upstream_model"
down_revision = "0003_claude_model_mappings"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("claude_model_mappings"):
        columns = {column["name"] for column in inspector.get_columns("claude_model_mappings")}
        if "upstream_model_id" not in columns:
            op.add_column(
                "claude_model_mappings",
                sa.Column("upstream_model_id", sa.String(), nullable=True),
            )
            op.execute(
                "UPDATE claude_model_mappings SET upstream_model_id = claude_model_id "
                "WHERE upstream_model_id IS NULL"
            )
            op.alter_column("claude_model_mappings", "upstream_model_id", nullable=False)
    # Separate per-mapping candidates are replaced by the referenced route.
    if inspector.has_table("claude_mapping_candidates"):
        op.drop_table("claude_mapping_candidates")


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("claude_model_mappings"):
        columns = {column["name"] for column in inspector.get_columns("claude_model_mappings")}
        if "upstream_model_id" in columns:
            op.drop_column("claude_model_mappings", "upstream_model_id")
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
