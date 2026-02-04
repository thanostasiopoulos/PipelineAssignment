from fastapi.testclient import TestClient

from app.main import app


def test_health_endpoint(tmp_path, monkeypatch):
    # create a tiny metrics file
    p = tmp_path / "metrics.csv"
    p.write_text(
        "event_ts,service,request_count,avg_latency_ms,status_count,error_rate,\n"
        "2025-01-12T10:32:00Z,checkout,2,200.00,2,0.50\n",
        encoding="utf-8",
    )

    monkeypatch.setattr("app.main.metrics_path", p)
    with TestClient(app) as c:
        r = c.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert body["metrics_rows"] == 1

        r = c.get("/metrics?service=checkout")
        assert r.status_code == 200
        assert len(r.json()) == 1
