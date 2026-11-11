"""The DASH_PASSWORD gate in front of every page of app.py."""
import os
import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DASH_SOURCE", "sample")
sys.path.insert(0, str(ROOT))

QUEUE_TITLE = "Which accounts should investigators review first?"


def run_app():
    at = AppTest.from_file(str(ROOT / "app.py"), default_timeout=60).run()
    assert not at.exception, [e.value for e in at.exception]
    return at


def titles(at):
    return [t.value for t in at.title]


def test_no_page_content_before_login(monkeypatch):
    monkeypatch.setenv("DASH_PASSWORD", "s3cret")
    at = run_app()
    assert len(at.text_input) == 1
    assert QUEUE_TITLE not in titles(at)
    assert not at.dataframe


def test_a_wrong_password_is_rejected(monkeypatch):
    monkeypatch.setenv("DASH_PASSWORD", "s3cret")
    at = run_app()
    at.text_input[0].input("guess").run()
    assert not at.exception, [e.value for e in at.exception]
    assert any("Wrong password" in e.value for e in at.error)
    assert QUEUE_TITLE not in titles(at)


def test_the_right_password_shows_the_default_page(monkeypatch):
    monkeypatch.setenv("DASH_PASSWORD", "s3cret")
    at = run_app()
    at.text_input[0].input("s3cret").run()
    assert not at.exception, [e.value for e in at.exception]
    assert QUEUE_TITLE in titles(at)
    assert not at.error
    at.run()  # success is remembered for the session
    assert QUEUE_TITLE in titles(at)


def test_no_gate_when_unset(monkeypatch):
    monkeypatch.delenv("DASH_PASSWORD", raising=False)
    at = run_app()
    assert QUEUE_TITLE in titles(at)
    assert not any(t.label == "Password" for t in at.text_input)
