import io
import zipfile
import pytest

from gdelt_server.store import Store, SLOT, utcnow


def zipped(rows):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("sample.tsv", "\n".join("\t".join(r) for r in rows))
    return output.getvalue()


def event_row(event_id="1", actor1="USA", actor2="CHN", geo="US", tone="-2", root="19", layout=61):
    row = [""]*layout
    for i, value in {0:event_id,6:"Actor A",7:actor1,16:"Actor B",17:actor2,
                     26:root,28:root,29:"4",30:"-10",31:"5",32:"2",33:"3",34:tone}.items():
        row[i] = value
    cc, date, url = (53,59,60) if layout == 61 else (51,56,57)
    row[cc],row[date],row[url] = geo,"20261006000000","https://example.org/article"
    return row


def gkg_row(record="1", countries=("US",), themes="ARMEDCONFLICT;ECON_TRADE;", org="Huawei", tone="-2"):
    row = [""]*27
    row[0],row[1],row[3],row[4] = record,"20261006000000","news source","https://example.org/article"
    row[7] = themes
    row[9] = ";".join(f"1#Place#{c}#0#0#0#0" for c in countries)
    row[13],row[15] = org, f"{tone},1,2,3,4,5,6"
    return row


@pytest.fixture
def store(tmp_path):
    result = Store(tmp_path/"gdelt.db")
    result.initialize()
    return result


@pytest.fixture
def recent_ts():
    return int(utcnow().timestamp())//SLOT*SLOT-8*SLOT
