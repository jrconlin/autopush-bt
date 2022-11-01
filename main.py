### This is a crappy testbed application for me to learn how to do stuff with
### BigTable. It's not meant to be production and is barely functional.
###
###

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
MESSAGE_FAMILY = "message_family" # presumes expiry of 1s.

client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)

router = instance.table("router")
message = instance.table("message")


def calc_ttl(now:datetime, ttl:int):
    ### convenience function for genering an integer TTL offset.
    return now + datetime.timedelta(seconds=ttl)


def get_chids(uaid:str, options:Values):
    ### fetch the chids for the given UAID returns a set of uaid#chid
    key = f"^{uaid}#.*"
    all_rows = message.read_rows(filter_=row_filters.RowFilterChain(
        filters=[
            # get everything that matches the key regex (starts with uaid)
            row_filters.RowKeyRegexFilter(key),
            # don't return the column data
            row_filters.StripValueTransformerFilter(True)
        ]
    ))
    result = []
    # is there no way to get the count of rows without iterating?
    for row in all_rows:
        result.append(row.row_key.decode())
    # BigTable loves to return duplicates.
    return list(set(result))


def create_uaid(connected_at: datetime, router_type:str, node_id:str, options:Values) -> str:
    ### create a base UAID, with no channels (mocking a `register`)
    now = datetime.datetime.utcnow()
    uaid = uuid.uuid4().hex

    row = router.direct_row(uaid)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="connected_at", value=int(connected_at.timestamp()), timestamp=now)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="router_type", value=router_type, timestamp=now)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="node_id", value=node_id, timestamp=now)
    # not sure we need this if we use column family expiry rules.
    # row.set_cell(column_family_id=DEFAULT_FAMILY, column="expiry", value=int(calc_ttl(now, 300).timestamp()), timestamp=now)

    print(f"Creating UAID: {uaid}")
    rr = row.commit()
    if rr.ListFields() != []:
        # `commit` can return errors. It does not raise exceptions.
        print(f"DIRECT COMMIT ERROR: {rr}")
    return uaid

def register_channel(uaid: str, options:Values) -> str:
    ### Mock registering a new channel.
    chid = uuid.uuid4().hex
    now = datetime.datetime.utcnow()
    key = f"{uaid}#{chid}"

    row = message.direct_row(key)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="created", value=int(now.timestamp()))
    rr = row.commit()

    print(f" created {key}")
    if rr.ListFields() != []:
        print(f"DIRECT COMMIT ERROR: {rr}")

    return key

def create_message(key:str, now:datetime):
    ### create a fake message under a given uaid#chid
    data = base64.urlsafe_b64encode(os.urandom(random.randint(2048,4096)))
    # static/fake headers
    headers = json.dumps({
        "crypto-key": "keyid=p256dh;dh=BDJKCfgkhmp-jPgyO9quOwiIVW4yVtdRveMYfEhTIO0u9SFnvcnCz0mIGG_Llgw0OVNzPO-XqaXRthfPW2Gb3Xg",
        "encryption": "keyid=p256dh;salt=uSa84w1mucrrFN6zMghtdQ",
        "content-encoding": "aesgcm",
    })
    # give it some ttl (in seconds)
    ttl = calc_ttl(now, random.randint(60,10000))

    row = message.direct_row(key)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="sortkey_timestamp", value=int(now.timestamp()), timestamp=now)
    row.set_cell(column_family_id=MESSAGE_FAMILY, column="data", value=data, timestamp=ttl)
    row.set_cell(column_family_id=MESSAGE_FAMILY, column="headers", value=headers, timestamp=ttl)
    print(f"  New message for {key} dies in {ttl-now}")
    res = row.commit()

    if res.ListFields() != []:
        print (f"DIRECT COMMIT ERROR: {res}")

def clear_chid(key: str):
    ### delete all data for this uaid#chid, mocks an `ack`
    print(f" Clearing {key}")

    row = message.row(key)
    row.delete_cells(column_family_id=MESSAGE_FAMILY, columns=["data", "headers"])
    res = row.commit()

    # so, if things fail, `commit` doesn't throw an exception. Instead, the result value contains the error.
    if res.ListFields() != []:
        print (f"CONDITION COMMIT ERROR: {res}")


def process_uaid(uaid:str, options:Values):
    ### mock a message check.
    chids = get_chids(uaid, options)
    for chid in chids:
        if random.randint(0, 1) == 1:
            clear_chid(chid)
        else:
            print(f"Skipping {chid}")


def load_uaid(uaid:str, options:Values):
    ### generate a few random updates to deliver
    chids = get_chids(uaid, options)
    now = datetime.datetime.utcnow()
    for i in range(0, random.randrange(1,10)):
        create_message(random.choice(chids), now=now)


def populate(options:Values):
    ### Stuff them tables with crap
    print("Writing router records...")
    uaids = []
    for i in range(1, options.populate):
        # create fake UAID
        now = datetime.datetime.utcnow()
        connected_at = now
        router_type = "webpush"
        node_id = "https://example.com:8082/" + uuid.uuid4().hex
        uaid = create_uaid(connected_at=connected_at, router_type=router_type, node_id=node_id, options=options)
        # register some channels
        chids = []
        msg_count = 0
        for i in range(0, random.randint(10)):
            chids.append(register_channel(uaid, options))
        # and populate them with messages.
        for j in range(0, random.randint(10)):
            chid = random.choice(chids)
            for k in range(1, random.randrange(1,5)):
                create_message(chid)
            msg_count += k
        print (f"{uaid}: Created {i} channels {msg_count} messages")


def get_pending(target_uaid:str, options:Values):
    ### get the pending messages, well, message sizes.
    filter = row_filters.RowKeyRegexFilter(f"^{target_uaid}#.*".encode("utf-8"))
    all_rows = message.read_rows(
        filter_=filter
    )
    results = []
    for row in all_rows:
        for _cf, cols in row.cells.items():
            for data in cols.get(b'data') or []:
                results.append((row.row_key.decode(), len(data.value)))
    return results

def connect_uaid(uaid:str, options:Values):
    ### Pretend a UAID connected
    print(f"checking {uaid}")
    node_id = "https://example.com:8082/" + uuid.uuid4().hex
    now = datetime.datetime.utcnow()

    row = router.direct_row(uaid)
    row.set_cell(
        column_family_id=DEFAULT_FAMILY,
        column="connected_at",
        value=int(now.timestamp()).to_bytes(4, "big"),
        timestamp=now
    )
    row.set_cell(
        column_family_id=DEFAULT_FAMILY,
        column="node_id",
        value=node_id,
        timestamp=now
    )
    rr = row.commit()

    """
    # append_cell copies the prior content, and appends data to it. It
    # returns it's own commit message error, because of course.
    if len(rr) > 0:
        print(f"APPEND COMMIT ERROR: {rr}")
    """
    # get the pending messages:
    messages = get_pending(uaid, options)
    count = 0
    for message in messages:
        count += 1
        print(f"{count}, {message}")


def print_row(row:row.PartialRowData):
    ### Taken from the example code.
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
                val = None
                labels = (
                    " [{}]".format(",".join(cell.labels)) if len(cell.labels) else ""
                )
                try:
                    match col.decode("utf-8"):
                        case "connected_at":
                            val = int.from_bytes(cell.value, "big")
                        case "expiry":
                            val = int.from_bytes(cell.value, "big")
                        case "sortkey_timestamp":
                            val = int.from_bytes(cell.value, "big")
                        case "created":
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


def dump_uaid(target_uaid):
    ### Dump info for a target_uaid
    # show the router info
    rrow = router.read_row(target_uaid)
    print_row(rrow)
    # show the "messages" that might be for this UAID.
    # this appears to match only cells that have this value, regardless of column name.
    #filter = row_filters.ValueRangeFilter(start_value=int.to_bytes(int(datetime.datetime.utcnow().timestamp()), length=8, byteorder="big"))
    filter = row_filters.RowKeyRegexFilter(f"^{target_uaid}#.*".encode("utf-8"))
    all_rows = message.read_rows(
        filter_=filter
    )
    ## wasn't super sure may revisit this. Specifying the start/end key may limit scan.
    # vv = router.read_rows(start_key="0", end_key="fffffffffffffffffffffff")
    # print(vv)
    for row in all_rows:
        print_row(row)


def get_uaids(limit: int=0):
    ### fetch all the UAIDs we have. Including broken ones.
    # this is actually pretty expensive. It's a table scan.
    # StripValue... replaces the values with empty strings.
    # (If this is hadoop like, it still fetches them, it just strips before sending)
    filter = row_filters.StripValueTransformerFilter(True)
    rows = router.read_rows(filter_=filter)
    result = [row.row_key for row in rows]
    if limit:
        return result[:limit]
    return result

def get_args():
    parser = OptionParser()
    parser.add_option("--populate", "-p", type=int, default=0, help="populate this many")
    parser.add_option("--uaid", type=str, help="target uaid")
    parser.add_option("--ack", type=str, help="ack a UAID#CHID")
    parser.add_option("--process", action="store_true", default=False, help="process a UAID")
    parser.add_option("--modify", "-m", action="store_true", default=False, help="modify")
    parser.add_option("--sleep", "-s", type=int, default=0, help="max sleep seconds between modifications")
    parser.add_option("--display", action="store_true", default=False, help="display uaid")
    parser.add_option("--register", action="store_true", default=False, help="register some channels")
    parser.add_option("--list_uaids", action="store_true", default=False, help="Display a list of known UAIDs")
    parser.add_option("--load", action="store_true", default=False, help="Load a UAID with fake messages")
    parser.add_option("--dump", action="store_true", default=False, help="Dump a UAID")
    parser.add_option("--drop", action="store_true", help="drop a UAID and all records")
    parser.add_option("--skip_create", action="store_true", default=False, help="Don't create new UAIDs")
    parser.add_option("--prune", action="store_true", default=False, help="Prune empty UAIDs")

    return parser.parse_args()

def drop_all(uaid):
    ### drop this UAID and everything we know about it. (surprisingly expensive.)
    print(f"dropping {uaid}", end="")
    now = time.time()
    router.drop_by_prefix(uaid)
    message.drop_by_prefix(uaid)
    duration = time.time() - now
    print(f" Duration {duration}")
    uaids = get_uaids()
    print(f" remaining: {uaids}")
    time.sleep(1)  # because there's a rate limit of ~100/m on row drops :facepalm:


def target_uaid(options:Values):
    ### return the `uaid` parameter, or just pick one at random.
    return options.uaid or random.choice(get_uaids())

def main():
    (options, _args) = get_args()

    if options.populate:
        uaids = populate(options)
        print(f"  UAIDS: {uaids}")
        exit()

    if options.ack:
        clear_chid(options.ack)
        exit()

    if options.process:
        process_uaid(target_uaid(options), options)
        exit()

    if options.load:
        load_uaid(target_uaid(options), options)
        exit()

    if options.drop:
        drop_all(target_uaid(options))
        exit()

    if options.register:
        target = target_uaid(options)
        for i in range(1, random.randrange(1,10)):
            register_channel(target, options)
        exit()

    if options.dump:
        uaids = get_uaids()
        for uaid in uaids:
            dump_uaid(uaid)
        exit()

    if options.list_uaids:
        uaids = get_uaids()
        print(f"  UAIDS: {uaids}")
        exit()

    if options.display:
        target = target_uaid(options)
        dump_uaid(target)
        now = time.time()
        chids = get_chids(target, options)
        print(f"  CHIDS: {chids}: {time.time() - now}")
        exit()

    if options.prune:
        uaids = get_uaids()
        for uaid in uaids:
            chids = get_chids(uaid, options)
            if not chids:
                drop_all(uaid)
        exit()

    connect_uaid(target_uaid(options), options)

main()