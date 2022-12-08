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

from typing import (Dict, List, Tuple);

from optparse import (OptionParser, Values)
from google.cloud import bigtable
from google.cloud.bigtable import (column_family, row_filters, row_set, row, table)
from google.cloud.bigtable.row_data import PartialRowsData

# try loading everything in one table.

PROJECT_ID = "autopush-dev"
INSTANCE_ID = "development-1"
DEFAULT_FAMILY = "default"
ROUTER_FAMILY = "router"
MESSAGE_FAMILY = "message" # presumes expiry of 1s.

client = bigtable.Client(project=PROJECT_ID, admin=True)
instance = client.instance(INSTANCE_ID)

autopush = instance.table("autopush")

def calc_ttl(now:datetime, ttl:int):
    ### convenience function for genering an integer TTL offset.
    return now + datetime.timedelta(seconds=ttl)


def get_chids(uaid:bytes, options:Values) -> List[bytes]:
    ### fetch the chids for the given UAID returns a set of uaid#chid
    key = f"^{uaid.decode()}#.*".encode()
    print(key)
    all_rows = autopush.read_rows(filter_=row_filters.RowFilterChain(
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
        if not row.to_dict().get(f"{DEFAULT_FAMILY}:dead".encode()):
            result.append(row.row_key)
    # BigTable loves to return duplicates.
    return list(set(result))


def create_uaid(connected_at: datetime, router_type:str, node_id:str, options:Values) -> bytes:
    ### create a base UAID, with no channels (mocking a `register`)
    now = datetime.datetime.utcnow()
    uaid = uuid.uuid4().hex.encode()

    row = autopush.direct_row(uaid)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="connected_at", value=int(connected_at.timestamp()), timestamp=now)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="router_type", value=router_type, timestamp=now)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="node_id", value=node_id, timestamp=now)
    # not sure we need this if we use column family expiry rules.

    print(f"Creating UAID: {uaid}", end="")
    rr = row.commit()
    print("✔")
    if rr.ListFields() != []:
        # `commit` can return errors. It does not raise exceptions.
        raise Exception(f"DIRECT COMMIT ERROR: {rr}")
    return uaid

def register_channel(uaid: bytes, options:Values) -> bytes:
    ### Mock registering a new channel.
    chid = uuid.uuid4().hex
    now = datetime.datetime.utcnow()
    key = f"{uaid.decode()}#{chid}".encode()

    row = autopush.direct_row(key)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="created", value=int(now.timestamp()))
    print("registering new channel ", end="")
    rr = row.commit()
    print("✔")

    print(f" created {key}")
    if rr.ListFields() != []:
        raise Exception(f"DIRECT COMMIT ERROR: {rr}")

    return key

def create_message(key:bytes, now:datetime):
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
    row = autopush.read_row(key)
    if row and row.to_dict().get(f"{DEFAULT_FAMILY}:dead".encode()):
        print(f"☠{key}:404")

    row = autopush.direct_row(key)
    row.set_cell(column_family_id=DEFAULT_FAMILY, column="sortkey_timestamp", value=int(now.timestamp()), timestamp=now)
    row.set_cell(column_family_id=MESSAGE_FAMILY, column="data", value=data, timestamp=ttl)
    row.set_cell(column_family_id=MESSAGE_FAMILY, column="headers", value=headers, timestamp=ttl)
    print(f"  New message for {key} dies in {ttl-now}", end="")
    res = row.commit()
    print("✔")

    if res.ListFields() != []:
        raise Exception(f"DIRECT COMMIT ERROR: {res}")

def clear_chid(key: bytes):
    ### delete all data for this uaid#chid, mocks an `ack`
    row = autopush.row(key)
    row.delete_cells(column_family_id=MESSAGE_FAMILY, columns=["sortkey_timestamp", "data", "headers"])
    print(f" Clearing {key}", end="")
    res = row.commit()
    print(f" ✔")
    # so, if things fail, `commit` doesn't throw an exception. Instead, the result value contains the error.
    if res.ListFields() != []:
        raise Exception(f"CONDITION COMMIT ERROR: {res}")

def purge(table:table.Table, key:bytes):
    ### if you delete all the columns from a row, bigtable will delete the row. This does not appear to be
    ### rate limited.
    ###
    print(f" ❌ purging {key}, ")
    read_row = table.read_row(key.decode())
    if not read_row or read_row.cells:
        read_row = table.read_row(key)
        if read_row is None or not read_row.cells:
            print(f"\n    Empty row {table.name}::{key}", end="")
            return
    del_row = table.row(key)
    # for each family, collect up the cells, and set them to delete.
    for family in read_row.cells.keys():
        columns = []
        for cell in read_row.cells.get(family).keys():
            columns.append(cell)
        print(f" {family}:{columns}, ", end="")
        del_row.delete_cells(column_family_id=family, columns=columns)
    res = del_row.commit()
    if res.ListFields():
        import pdb;pdb.set_trace()
        print(res)


def process_uaid(uaid:bytes, options:Values):
    ### mock a message check.
    chids = get_chids(uaid, options)
    for chid in chids:
        if random.randint(0, 1) == 1:
            clear_chid(chid)
        else:
            print(f"Skipping {chid}")


def load_uaid(uaid:bytes, options:Values):
    ### generate a few random updates to deliver
    now = datetime.datetime.utcnow()
    chids = get_chids(uaid, options)
    if not chids:
        for i in range(0, random.randint(1, 10)):
            chids.append(register_channel(uaid, options))
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
        for i in range(0, random.randint(1, 10)):
            chids.append(register_channel(uaid, options))
        # and populate them with messages.
        for j in range(0, random.randint(1, 10)):
            chid = random.choice(chids)
            for k in range(1, random.randint(1,5)):
                create_message(chid, now)
            msg_count += k
        print (f"{uaid}: Created {i} channels {msg_count} messages")
    return(uaids)


def get_pending(uaid:bytes, options:Values) -> List[Tuple[bytes, int]]:
    ### get the pending messages, well, message sizes.
    filter = row_filters.RowKeyRegexFilter(f"^{uaid.decode()}#.*".encode("utf-8"))
    all_rows = autopush.read_rows(
        filter_=filter
    )

    results = []
    for row in all_rows:
        for _cf, cols in row.cells.items():
            if cols.get(b'dead'):
                next
            for data in cols.get(b'data') or []:
                results.append((row.row_key, len(data.value)))
    return results

def get_cell(row:PartialRowsData, family: str, cell: str):
    if not row or row.cells:
        return None
    if not row.cells.get(family):
        return None
    return row.cells.get(family).get(cell)

def connect_uaid(uaid:bytes, options:Values):
    ### Pretend a UAID connected
    print(f"checking {uaid}", end="")
    node_id = "https://example.com:8082/" + uuid.uuid4().hex
    now = datetime.datetime.utcnow()

    read_row = autopush.read_row(uaid)
    if not read_row:
        print(f"\n############### 404 {uaid}")
        return
    now = datetime.datetime.utcnow()
    write_row = autopush.row(uaid)
    write_row.set_cell(column_family_id="router", column="connected_at", value=int(now.timestamp()), timestamp=now)
    write_row.set_cell(column_family_id="router", column="router_type", value=get_cell(read_row, "router", "router_type") or "", timestamp=now)
    write_row.set_cell(column_family_id="router", column="router_data", value=get_cell(read_row, "router", "router_data") or "", timestamp=now)
    write_row.set_cell(column_family_id="router", column="node_id", value=get_cell(read_row, "router", "node_id") or "", timestamp=now)
    write_row.set_cell(column_family_id="router", column="record_version", value=get_cell(read_row, "router", "record_version") or "", timestamp=now)
    rr = write_row.commit()
    print(f"✔")

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
        print(f"{count:>3}, {message}")


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


def dump_uaid(uaid:bytes):
    ### Dump info for a target
    # show the router info
    rrow = autopush.read_row(uaid)
    print_row(rrow)
    # show the "messages" that might be for this UAID.
    # this appears to match only cells that have this value, regardless of column name.
    #filter = row_filters.ValueRangeFilter(start_value=int.to_bytes(int(datetime.datetime.utcnow().timestamp()), length=8, byteorder="big"))
    uaid = f"{uaid.decode()}#.*".encode()
    filter = row_filters.RowKeyRegexFilter(uaid)
    all_rows = autopush.read_rows(
        filter_=filter
    )
    ## wasn't super sure may revisit this. Specifying the start/end key may limit scan.
    # vv = autopush.read_rows(start_key="0", end_key="fffffffffffffffffffffff")
    # print(vv)
    for row in all_rows:
        print_row(row)


def get_uaids(limit: int=0) -> List[bytes]:
    ### fetch all the UAIDs we have. Including broken ones.
    # this is actually pretty expensive. It's a table scan.
    # StripValue... replaces the values with empty strings.
    # (If this is hadoop like, it still fetches them, it just strips before sending)
    filter = row_filters.StripValueTransformerFilter(True)
    rows = autopush.read_rows(filter_=filter, limit=limit)
    result = [row.row_key for row in rows]
    return result


def drop_all(uaid:bytes, options:Values):
    ### drop this UAID and everything we know about it. (surprisingly expensive.)
    print(f"✖ dropping {uaid}")
    now = datetime.datetime.utcnow()
    # There's a limit on how many `drop_by_prefix` we can do.
    # bonus! this is also very slow.
    # autopush.drop_by_prefix(uaid)
    for chid in get_chids(uaid, options):
        purge(autopush, chid)
    purge(autopush, uaid)
    duration = datetime.datetime.utcnow() - now
    print(f" Duration {duration}")
    uaids = get_uaids()
    print(f" remaining: {uaids}")

def drop_chid(chid:bytes):
    ### clean out a chid by purging it.
    purge(autopush, chid)
    print("✔")


def target_uaid(options:Values) -> bytes:
    ### return the `uaid` parameter, or just pick one at random.
    return options.uaid.encode() or random.choice(get_uaids())


def register(options:Values) -> Dict:
    # register a UAID and add some random channels.
    now = datetime.datetime.utcnow()
    connected_at = now
    router_type = "webpush"
    node_id = "https://example.com:8082/" + uuid.uuid4().hex
    uaid = create_uaid(connected_at=connected_at, router_type=router_type, node_id=node_id, options=options)
    chids = []
    for i in range(0, random.randint(1, 10)):
        chids.append(register_channel(uaid, options))
    return {"uaid": uaid, "chids": chids}

def stress_test(options:Values):
    ### let's see what we can abuse.
    allchids = get_chids(".*".encode(), options)
    for i in range(1, options.stress):
        print(f"{i:>5} ##### ", end="")
        # register a new uaid and some channels
        if not allchids:
            user = register(options)
            allchids.extend(user.get("chids"))
            continue
        uaid = random.choice(allchids).decode().split('#')[0].encode()
        match random.randint(1,10):
            case 1:  # new user
                user = register(options)
                allchids.extend(user.get("chids"))
            case 2:  # connect, fetch messages, get list of chids, ack messages.
                connect_uaid(uaid, options)
                chids_str = get_chids(uaid, options)
                for chid in chids_str:
                    clear_chid(chid)
            case 3:  # connect and register a new channel
                connect_uaid(uaid, options)
                allchids.append(register_channel(uaid, options))
            case 4:  # drop a user
                drop_all(uaid, options)
                allchids = get_chids(".*".encode(), options)
            case 5:  # drop a chid
                drop_chid(random.choice(allchids))
            case other:  # send a message.
                create_message(random.choice(allchids), datetime.datetime.utcnow())

def get_args():
    parser = OptionParser()
    parser.add_option("--populate", "-p", type=int, default=0, help="populate this many")
    parser.add_option("--uaid", type=str, help="target uaid")
    parser.add_option("--delete", type=str, help="Clear a key")
    parser.add_option("--purge", action="store_true", default=False, help="burn it all...")
    parser.add_option("--ack", type=str, help="ack a UAID#CHID")
    parser.add_option("--process", action="store_true", default=False, help="process a UAID")
    parser.add_option("--modify", "-m", action="store_true", default=False, help="modify")
    parser.add_option("--sleep", "-s", type=int, default=0, help="max sleep seconds between modifications")
    parser.add_option("--display", action="store_true", default=False, help="display uaid")
    parser.add_option("--register", action="store_true", default=False, help="register some channels")
    parser.add_option("--list_chids", action="store_true", default=False, help="Display a list of known CHIDs for a UAID")
    parser.add_option("--list_uaids", action="store_true", default=False, help="Display a list of known UAIDs")
    parser.add_option("--load", action="store_true", default=False, help="Load a UAID with fake messages")
    parser.add_option("--dump", action="store_true", default=False, help="Dump databases")
    parser.add_option("--drop", action="store_true", help="drop a UAID and all records")
    parser.add_option("--skip_create", action="store_true", default=False, help="Don't create new UAIDs")
    parser.add_option("--prune", action="store_true", default=False, help="Prune empty UAIDs")
    parser.add_option("--stress", type=int, default=0, help="stress test bigtable")

    return parser.parse_args()


def main():
    (options, _args) = get_args()

    if options.stress:
        stress_test(options)
        return()

    if options.populate:
        uaids = populate(options)
        print(f"  UAIDS: {uaids}")
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.delete:
        target = options.delete.encode()
        print(f" deleting {options.delete} ", end="")
        purge(autopush, target)
        if '#' not in options.delete:
            purge(autopush, target)
        print ("")
        return()

    if options.purge:
        print("Purging...")
        for uaid in get_uaids():
            print(f" Deleting {uaid} ", end="")
            drop_all(uaid, options)
            print ("")

    if options.ack:
        clear_chid(options.ack)
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.process:
        process_uaid(target_uaid(options), options)
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.load:
        load_uaid(target_uaid(options), options)
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.drop:
        drop_all(target_uaid(options))
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.register:
        target = target_uaid(options)
        for i in range(1, random.randrange(1,10)):
            register_channel(target, options)
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    if options.dump:
        uaids = get_uaids()
        for uaid in uaids:
            dump_uaid(uaid.encode())
        return()

    if options.list_uaids:
        uaids = get_uaids()
        print(f"  UAIDs: {uaids}\nDuration {datetime.datetime.utcnow() - start}")
        return()

    if options.list_chids:
        chids = get_chids(target_uaid(options), options)
        print(f"  CHIDs: {chids}\nDuration {datetime.datetime.utcnow() - start}")
        return()


    if options.display:
        target = target_uaid(options)
        dump_uaid(target)
        chids = get_chids(target, options)
        print(f"  CHIDS: {chids}:\nDuration {datetime.datetime.utcnow() - start}")
        return()

    if options.prune:
        uaids = get_uaids()
        for uaid in uaids:
            chids = get_chids(uaid, options)
            if not chids:
                drop_all(uaid, options)
        print(f"Duration {datetime.datetime.utcnow() - start}")
        return()

    # connect_uaid(target_uaid(options), options)

start = datetime.datetime.utcnow()
main()
print(f"Duration {datetime.datetime.utcnow() - start}")
