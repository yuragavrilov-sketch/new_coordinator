"""Shared utilities for route handlers."""

from functools import wraps

from flask import jsonify, request
from pydantic import BaseModel, ValidationError


def db_required(state_ref: dict):
    """Decorator that returns 503 if database is unavailable.

    Usage:
        @bp.get("/api/foo")
        @db_required(_state)
        def get_foo():
            ...
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not state_ref.get("db_available", {}).get("value", False):
                return jsonify({"error": "DB unavailable"}), 503
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def validate_body(schema_cls: type[BaseModel]):
    """Parse request.json through a Pydantic schema.

    Returns the validated model instance.
    Raises a tuple (json_response, 400) on validation error.
    """
    try:
        return schema_cls.model_validate(request.json or {})
    except ValidationError as e:
        from flask import abort
        abort(400, description=e.errors())


def error_response(message: str, status: int = 400):
    """Return a standard JSON error response."""
    return jsonify({"error": message}), status


def register_error_handlers(app):
    """Register standard JSON error handlers on a Flask app."""

    @app.errorhandler(400)
    def bad_request(e):
        desc = e.description if hasattr(e, 'description') else str(e)
        return jsonify({"error": desc}), 400

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "not found"}), 404

    @app.errorhandler(409)
    def conflict(e):
        desc = e.description if hasattr(e, 'description') else str(e)
        return jsonify({"error": desc}), 409

    @app.errorhandler(500)
    def server_error(e):
        return jsonify({"error": "internal server error"}), 500
