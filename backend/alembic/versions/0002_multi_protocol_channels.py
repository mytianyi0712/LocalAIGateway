"""Add multi-protocol channel capability tables."""

import sqlalchemy as sa
from alembic import op


revision = "0002_multi_protocol_channels"
down_revision = "0001_initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table("channel_protocols"):
        op.create_table(
            "channel_protocols",
            sa.Column("channel_id", sa.String(), nullable=False),
            sa.Column("protocol", sa.String(), nullable=False),
            sa.ForeignKeyConstraint(["channel_id"], ["channels.id"], ondelete="CASCADE"),
            sa.PrimaryKeyConstraint("channel_id", "protocol"),
        )
    if not inspector.has_table("channel_model_protocols"):
        op.create_table(
            "channel_model_protocols",
            sa.Column("channel_model_id", sa.String(), nullable=False),
            sa.Column("protocol", sa.String(), nullable=False),
            sa.ForeignKeyConstraint(
                ["channel_model_id"], ["channel_models.id"], ondelete="CASCADE"
            ),
            sa.PrimaryKeyConstraint("channel_model_id", "protocol"),
        )


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if inspector.has_table("channel_model_protocols"):
        op.drop_table("channel_model_protocols")
    if inspector.has_table("channel_protocols"):
        op.drop_table("channel_protocols")
