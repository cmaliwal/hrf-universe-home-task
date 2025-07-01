"""add_days_to_hire_stats_table

Revision ID: b97077496e90
Revises: 991ecb2bf269
Create Date: 2025-06-27 13:51:39.211126

"""
from alembic import op
import sqlalchemy as sa


revision = 'b97077496e90'
down_revision = '991ecb2bf269'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('days_to_hire_stats',
    sa.Column('id', sa.String(), nullable=False),
    sa.Column('standard_job_id', sa.String(), nullable=False),
    sa.Column('country_code', sa.String(), nullable=True),
    sa.Column('min_days', sa.Float(), nullable=False),
    sa.Column('avg_days', sa.Float(), nullable=False),
    sa.Column('max_days', sa.Float(), nullable=False),
    sa.Column('job_postings_number', sa.Integer(), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    schema='public'
    )


def downgrade() -> None:
    op.drop_table('days_to_hire_stats', schema='public')
