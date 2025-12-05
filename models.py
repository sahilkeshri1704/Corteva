from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date, UniqueConstraint

Base = declarative_base()

class Weather(Base):
    __tablename__ = "weather"
    id = Column(Integer, primary_key=True)
    station = Column(String, index=True)
    date = Column(Date, index=True)
    tmax = Column(Float)
    tmin = Column(Float)
    precip = Column(Float)
    __table_args__ = (UniqueConstraint("station", "date"),)

class Stats(Base):
    __tablename__ = "stats"
    id = Column(Integer, primary_key=True)
    station = Column(String, index=True)
    year = Column(Integer, index=True)
    avg_tmax = Column(Float)
    avg_tmin = Column(Float)
    total_precip = Column(Float)
    __table_args__ = (UniqueConstraint("station", "year"),)
