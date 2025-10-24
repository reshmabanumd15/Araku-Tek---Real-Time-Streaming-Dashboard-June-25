from pathlib import Path

def test_schema_files():
    assert (Path('src/schemas/order_event.json').exists())
    assert (Path('src/schemas/click_event.json').exists())
