import pytest
from flask import Flask
from pydantic import BaseModel, Field

from routes.utils import db_required, validate_body, register_error_handlers


class SampleSchema(BaseModel):
    name: str = Field(min_length=1)
    count: int = 1


@pytest.fixture
def app():
    app = Flask(__name__)
    register_error_handlers(app)

    state = {"db_available": {"value": True}}

    @app.get("/test-db")
    @db_required(state)
    def test_endpoint():
        return {"ok": True}

    @app.post("/test-validate")
    def test_validate():
        data = validate_body(SampleSchema)
        return {"name": data.name, "count": data.count}

    return app, state


def test_db_required_passes(app):
    flask_app, state = app
    with flask_app.test_client() as c:
        resp = c.get("/test-db")
        assert resp.status_code == 200
        assert resp.json == {"ok": True}


def test_db_required_503(app):
    flask_app, state = app
    state["db_available"]["value"] = False
    with flask_app.test_client() as c:
        resp = c.get("/test-db")
        assert resp.status_code == 503
        assert "unavailable" in resp.json["error"].lower()


def test_validate_body_valid(app):
    flask_app, _ = app
    with flask_app.test_client() as c:
        resp = c.post("/test-validate", json={"name": "test", "count": 5})
        assert resp.status_code == 200
        assert resp.json == {"name": "test", "count": 5}


def test_validate_body_invalid(app):
    flask_app, _ = app
    with flask_app.test_client() as c:
        resp = c.post("/test-validate", json={"name": ""})
        assert resp.status_code == 400


def test_validate_body_defaults(app):
    flask_app, _ = app
    with flask_app.test_client() as c:
        resp = c.post("/test-validate", json={"name": "hello"})
        assert resp.status_code == 200
        assert resp.json["count"] == 1
