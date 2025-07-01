import uuid
from statistics import mean, quantiles
from typing import List, Dict, Optional
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from home_task.db import get_session
from home_task.models import JobPosting, DaysToHireStats


def calculate_stats(values: List[int], min_postings: int = 5) -> Optional[Dict]:
    """Calculate statistics from a list of days to hire values.
    
    Args:
        values: List of days to hire values
        min_postings: Minimum number of postings required
    
    Returns:
        Dictionary with min, max, avg days and posting count, or None if insufficient data
    """
    if len(values) < min_postings:
        return None
    
    sorted_values = sorted(values)
    min_days, max_days = quantiles(sorted_values, n=10)[0], quantiles(sorted_values, n=10)[-1]
    filtered_values = [v for v in sorted_values if min_days <= v <= max_days]
    
    return {
        'min_days': float(min_days),
        'max_days': float(max_days),
        'avg_days': float(mean(filtered_values)),
        'job_postings_number': len(values)
    }


def process_chunk(session, query, chunk_size: int = 1000) -> None:
    """Process a chunk of data and insert statistics.
    
    Args:
        session: SQLAlchemy session
        query: Base query to execute
        chunk_size: Size of each chunk to process
    """
    offset = 0
    while True:
        # Get chunk of data
        chunk = session.execute(
            query.limit(chunk_size).offset(offset)
        ).fetchall()
        
        if not chunk:
            break
            
        # Process each row in chunk
        for row in chunk:
            if stats := calculate_stats(row.days_to_hire):
                session.execute(
                    insert(DaysToHireStats).values(
                        id=str(uuid.uuid4()),
                        standard_job_id=row.standard_job_id,
                        country_code=getattr(row, 'country_code', None),
                        **stats
                    )
                )
        
        # Commit chunk and update offset
        session.commit()
        offset += chunk_size


def update_stats(session) -> None:
    """Update statistics in the database using SQLAlchemy ORM efficiently.
    
    Args:
        session: SQLAlchemy session
    """
    try:
        # Clear existing stats within transaction
        session.execute(text('TRUNCATE TABLE days_to_hire_stats'))
        session.commit()
        
        # Base query for aggregating days_to_hire
        base_query = (
            select(
                JobPosting.standard_job_id,
                JobPosting.country_code,
                func.array_agg(JobPosting.days_to_hire).label('days_to_hire'),
                func.count().label('posting_count')
            )
            .where(JobPosting.days_to_hire.isnot(None))
            .group_by(JobPosting.standard_job_id, JobPosting.country_code)
            .having(func.count() >= 5)
        )
        
        # Process country-specific stats
        country_query = base_query.order_by(
            JobPosting.standard_job_id,
            JobPosting.country_code
        )
        process_chunk(session, country_query)
        
        # Process global stats (NULL country_code)
        global_query = (
            select(
                JobPosting.standard_job_id,
                func.array_agg(JobPosting.days_to_hire).label('days_to_hire'),
                func.count().label('posting_count')
            )
            .where(JobPosting.days_to_hire.isnot(None))
            .group_by(JobPosting.standard_job_id)
            .having(func.count() >= 5)
            .order_by(JobPosting.standard_job_id)
        )
        process_chunk(session, global_query)
        
    except Exception as e:
        session.rollback()
        raise e


if __name__ == '__main__':
    with get_session() as session:
        update_stats(session) 