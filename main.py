import os
from datetime import datetime
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, func, extract
from sqlalchemy.orm import sessionmaker
from models import Base, Weather, Stats

engine = create_engine("sqlite:///weather.db")
Session = sessionmaker(bind=engine)
Base.metadata.create_all(engine)

DATA_DIR = "wx_data"

# ---------------- Ingest ---------------- #
def ingest():
    s = Session()
    total = 0
    for f in os.listdir(DATA_DIR):
        path = os.path.join(DATA_DIR, f)
        station = f.replace(".txt", "")

        for line in open(path):
            d, mx, mn, pr = line.strip().split("\t")
            date = datetime.strptime(d, "%Y%m%d").date()

            mx = None if mx == "-9999" else float(mx)/10
            mn = None if mn == "-9999" else float(mn)/10
            pr = None if pr == "-9999" else float(pr)/100  # mm → cm

            if not s.query(Weather).filter_by(station=station, date=date).first():
                s.add(Weather(station=station, date=date, tmax=mx, tmin=mn, precip=pr))
                total += 1

    s.commit()
    s.close()
    print(f"Ingested: {total}")


# ---------------- Stats ---------------- #
def calc_stats():
    s = Session()
    stations = s.query(Weather.station).distinct()

    for (st,) in stations:
        years = s.query(extract("year", Weather.date)).filter_by(station=st).distinct()

        for (yr,) in years:
            avg_max = s.query(func.avg(Weather.tmax)).filter_by(station=st).filter(extract("year", Weather.date)==yr).scalar()
            avg_min = s.query(func.avg(Weather.tmin)).filter_by(station=st).filter(extract("year", Weather.date)==yr).scalar()
            total_p = s.query(func.sum(Weather.precip)).filter_by(station=st).filter(extract("year", Weather.date)==yr).scalar()

            s.merge(Stats(station=st, year=yr, avg_tmax=avg_max, avg_tmin=avg_min, total_precip=total_p))

    s.commit()
    s.close()
    print("Stats calculated")


# ---------------- API ---------------- #
app = Flask(__name__)

@app.get("/api/weather")
def get_weather():
    s = Session()
    q = s.query(Weather)

    if sid := request.args.get("station"):
        q = q.filter(Weather.station == sid)
    if dt := request.args.get("date"):
        q = q.filter(Weather.date == dt)

    data = [{
        "station": w.station,
        "date": str(w.date),
        "tmax": w.tmax,
        "tmin": w.tmin,
        "precip": w.precip
    } for w in q.limit(100)]

    s.close()
    return jsonify(data)

@app.get("/api/weather/stats")
def get_stats():
    s = Session()
    q = s.query(Stats)

    if sid := request.args.get("station"):
        q = q.filter(Stats.station == sid)
    if yr := request.args.get("year"):
        q = q.filter(Stats.year == int(yr))

    data = [{
        "station": st.station,
        "year": st.year,
        "avg_tmax": st.avg_tmax,
        "avg_tmin": st.avg_tmin,
        "total_precip": st.total_precip
    } for st in q.limit(100)]

    s.close()
    return jsonify(data)


if __name__ == "__main__":
    import sys
    if "ingest" in sys.argv:
        ingest()
    elif "stats" in sys.argv:
        calc_stats()
    else:
        app.run(debug=True)
