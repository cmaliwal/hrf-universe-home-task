from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from sqlalchemy import select

from home_task.db import get_session
from home_task.models import DaysToHireStats

app = FastAPI(title="Days to Hire Statistics API")

@app.get("/stats")
async def get_stats(standard_job_id: str, country_code: Optional[str] = None) -> Dict:
    """Get hiring statistics for a specific job and optionally a specific country.
    
    Args:
        standard_job_id: ID of the standard job to get statistics for
        country_code: Optional country code to filter statistics by
    
    Returns:
        Dictionary containing min, max, average days to hire and number of job postings
    
    Raises:
        HTTPException: If statistics are not found or on server error
    """
    with get_session() as session:
        try:
            query = select(DaysToHireStats).where(
                DaysToHireStats.standard_job_id == standard_job_id,
                DaysToHireStats.country_code == country_code
            )
            result = session.execute(query).scalar()

            if not result:
                raise HTTPException(status_code=404, detail="Statistics not found")

            return {
                "standard_job_id": result.standard_job_id,
                "country_code": result.country_code,
                "min_days": float(result.min_days),
                "avg_days": float(result.avg_days),
                "max_days": float(result.max_days),
                "job_postings_number": result.job_postings_number,
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) 