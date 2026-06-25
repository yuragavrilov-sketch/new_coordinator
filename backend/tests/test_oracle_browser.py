from __future__ import annotations

from db import oracle_browser


class CursorStub:
    def __init__(self, executed: list[str]):
        self.executed = executed

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, *_args, **_kwargs):
        self.executed.append(sql)


class ConnStub:
    def __init__(self):
        self.executed: list[str] = []
        self.committed = False

    def cursor(self):
        return CursorStub(self.executed)

    def commit(self):
        self.committed = True


class TableInfoCursorStub:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, *_args, **_kwargs):
        self.sql = sql.lower()

    def fetchall(self):
        if "all_tab_columns" in self.sql:
            return [("ID", "NUMBER", "N")]
        if "constraint_type = 'p'" in self.sql:
            return [("ID",)]
        return []

    def fetchone(self):
        if "v$database" in self.sql:
            return ("NO",)
        if "all_log_groups" in self.sql:
            return (1,)
        return None


class TableInfoConnStub:
    def cursor(self):
        return TableInfoCursorStub()


def test_get_table_info_reports_table_supplemental_logging():
    info = oracle_browser.get_table_info(TableInfoConnStub(), "SRC", "T")

    assert info["columns"] == [{"name": "ID", "type": "NUMBER", "nullable": False}]
    assert info["pk_columns"] == ["ID"]
    assert info["supplemental_log_data_all"] == "YES"


def test_enable_all_disabled_objects_enables_fk_novalidate(monkeypatch):
    monkeypatch.setattr(oracle_browser, "is_temporary_table", lambda *_args: False)
    monkeypatch.setattr(
        oracle_browser,
        "get_full_ddl_info",
        lambda *_args: {
            "indexes": [{"name": "IX_CHILD", "status": "UNUSABLE"}],
            "constraints": [
                {"name": "CHK_CHILD", "type_code": "C", "status": "DISABLED"},
                {"name": "FK_CHILD_PARENT", "type_code": "R", "status": "DISABLED"},
            ],
        },
    )
    conn = ConnStub()

    result = oracle_browser.enable_all_disabled_objects(conn, "TGT", "CHILD")

    assert 'ALTER INDEX "TGT"."IX_CHILD" REBUILD NOLOGGING' in conn.executed
    assert 'ALTER TABLE "TGT"."CHILD" ENABLE CONSTRAINT "CHK_CHILD"' in conn.executed
    assert 'ALTER TABLE "TGT"."CHILD" ENABLE NOVALIDATE CONSTRAINT "FK_CHILD_PARENT"' in conn.executed
    assert result["enabled"]["constraints"] == ["CHK_CHILD"]
    assert result["enabled"]["fk_novalidate"] == ["FK_CHILD_PARENT"]
    assert result["errors"] == {"indexes": [], "constraints": []}
    assert conn.committed
