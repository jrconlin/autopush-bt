import datetime
import uuid
import random
import time
import base64
import os
import json
import sys

from typing import List;

from optparse import (OptionParser, Values)
from google.cloud import bigtable
from google.cloud.bigtable import (column_family, row_filters, row_set, row)

PROJECT_ID = "autopush-dev"
INSTANCE_ID = "development-1"
DEFAULT_FAMILY = "default_family"



client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)

router = instance.table("router")
message = instance.table("message")
uaids = []
rows = []


def calc_ttl(now:datetime, ttl:int) -> int:
    return int((now + datetime.timedelta(seconds=ttl)).timestamp())


def populate_chids(uaid:str, options:Values):
    print("Creating 'channels'")
    for i in range(1, random.randint(2,10)):
        now = datetime.datetime.utcnow()
        chid = uuid.uuid4().hex
        key = f"{uaid}#{chid}"
        for j in range(0, random.randint(1,4)):

            row = message.direct_row(key)
            row.set_cell(column_family_id=DEFAULT_FAMILY, column="sortkey_timestamp", value=i, timestamp=now)
            data = base64.urlsafe_b64encode(os.urandom(random.randint(2048,4096)))
            headers = json.dumps({
                "crypto-key": "keyid=p256dh;dh=BDJKCfgkhmp-jPgyO9quOwiIVW4yVtdRveMYfEhTIO0u9SFnvcnCz0mIGG_Llgw0OVNzPO-XqaXRthfPW2Gb3Xg",
                "encryption": "keyid=p256dh;salt=uSa84w1mucrrFN6zMghtdQ",
                "content-encoding": "aesgcm",
            })
            row.set_cell(column_family_id="message_family", column="data", value=data, timestamp=now)
            row.set_cell(column_family_id="message_family", column="headers", value=headers, timestamp=now)
            row.set_cell(column_family_id="message_family", column="ttl", value=calc_ttl(now, random.randint(60,10000)), timestamp=now)
            res = row.commit()
            if res.ListFields() != []:
                print (res)
    print (f"{uaid}: Created {i} channels {i+j} messages")


def populate(options:Values):
    print("Writing router records...")

    for i in range(1,10):
        # create fake UAID
        now = datetime.datetime.utcnow()
        uaid = uuid.uuid4().hex
        uaids.append(uaid)
        connected_at = now
        router_type = "webpush"
        node_id = "https://example.com:8082/" + uuid.uuid4().hex
        row = router.direct_row(uaid)
        row.set_cell(column_family_id=DEFAULT_FAMILY, column="connected_at", value=int(connected_at.timestamp()), timestamp=now)
        row.set_cell(column_family_id=DEFAULT_FAMILY, column="router_type", value=router_type, timestamp=now)
        row.set_cell(column_family_id=DEFAULT_FAMILY, column="node_id", value=node_id, timestamp=now)
        row.set_cell(column_family_id=DEFAULT_FAMILY, column="expiry", value=calc_ttl(now, 300), timestamp=now)
        rr = row.commit()
        if rr.ListFields() != []:
            print(rr)
        rows.append(row)

        # create fake chids
        populate_chids(uaid, options)


    if options.m:
        for i in range (1, 20):
            uaid = random.choice(uaids)
            print(f"{i:>3}: tweaking {uaid}")
            node_id = "https://example.com:8082/" + uuid.uuid4().hex
            now = datetime.datetime.utcnow()
            try:
                if options.m:
                    row = router.append_row(uaid)
                    row.append_cell_value(column_family_id=DEFAULT_FAMILY, column="connected_at", value=int(connected_at.timestamp()).to_bytes(4, "big"))
                    row.append_cell_value(column_family_id=DEFAULT_FAMILY, column="node_id", value=node_id)
                    rr = row.commit()
                    if len(rr) > 0:
                        print(rr)
                else:
                    row = router.direct_row(uaid)
                    row.set_cell(column_family_id=DEFAULT_FAMILY, column="connected_at", value=int(connected_at.timestamp()), timestamp=now)
                    row.set_cell(column_family_id=DEFAULT_FAMILY, column="node_id", value=node_id, timestamp=now)
                    rr = row.commit()
                    if rr != "":
                        print(rr)
            except Exception as ex:
                print (ex)
            #rr = router.mutate_rows([row])
            #if rr != "":
            #    print(rr)
            sleep_period = random.randrange(options.s)
            print(f"Sleeping {sleep_period} seconds")
            time.sleep(sleep_period)

    return(uaids)


def print_row(row:row.PartialRowData):
    print("Reading data for {}:".format(row.row_key.decode("utf-8")))
    """
    odict_items(
        [
            ('expiry',
                OrderedDict(
                    [
                        (b'expiry', [<Cell value=b'1666184265' timestamp=2022-10-19 05:52:45.901000+00:00>]
                        )
                    ]
                )
            ),
            ('default',
                OrderedDict(
                    [
                        (b'connected_at', [<Cell value=b'1666183965' timestamp=2022-10-19 05:52:45.901000+00:00>]),
                        (b'node_id', [<Cell value=b'https://example.com:8082/aa0c9789efc24130b73edab825570d65' timestamp=2022-10-19 05:52:45.901000+00:00>]),
                        (b'router_type', [<Cell value=b'webpush' timestamp=2022-10-19 05:52:45.901000+00:00>])
                    ]
                )
            )
        ]
    )
    """

    for cf, cols in sorted(row.cells.items()):
        print("Column Family {}".format(cf))
        for col, cells in sorted(cols.items()):
            for cell in cells:
                labels = (
                    " [{}]".format(",".join(cell.labels)) if len(cell.labels) else ""
                )
                try:
                    match col.decode("utf-8"):
                        case "connected_at":
                            val = int.from_bytes(cell.value, "big")
                        case "expiry":
                            val = int.from_bytes(cell.value, "big")
                        case "data":
                            try:
                                val = "{} bytes ({} decoded)".format(len(cell.value), len(base64.urlsafe_b64decode(cell.value)))
                            except Exception as ex:
                                cell.value
                        case "ttl":
                            val = int.from_bytes(cell.value, "big")
                        case other:
                            val = cell.value.decode("utf-8")
                except Exception as ex:
                    import pdb; pdb.set_trace()
                    print(col.decode("utf-8"))

                print(
                    "\t{}: {} @{}{}".format(
                        col.decode("utf-8"),
                        val,
                        cell.timestamp,
                        labels,
                    )
                )
    print("")


def dump_row(target_uaid):
    rrow = router.read_row(target_uaid)
    print_row(rrow)
    # this appears to match only cells that have this value, regardless of column name.
    #filter = row_filters.ValueRangeFilter(start_value=int.to_bytes(int(datetime.datetime.utcnow().timestamp()), length=8, byteorder="big"))
    filter = row_filters.RowKeyRegexFilter(f"^{target_uaid}#.*".encode("utf-8"))
    vv = message.read_rows(
        filter_=filter
    )
    #print(set)
    # vv = router.read_rows(start_key="0", end_key="fffffffffffffffffffffff")
    # print(vv)
    for row in vv:
        print_row(row)


def get_uaids():
    filter = row_filters.StripValueTransformerFilter(True)
    rows = router.read_rows(filter_=filter)
    # list comprehension doesn't appear to work here.
    return [row.row_key for row in rows]


def args():
    parser = OptionParser()
    parser.add_option("-p", action="store_true", default=False, help="populate")
    parser.add_option("-m", action="store_true", default=False, help="modify")
    parser.add_option("-s", type=int, default=10, help="sleep max")
    return parser.parse_args()


(options, args) = args()
if options.p:
    uaids = populate(options)
else:
    uaids = get_uaids()
target_uaid = random.choice(uaids)
print(f"Picking {target_uaid} from {len(uaids)} records")
dump_row(target_uaid)
