import logging
import os
import re
import sys
import time
import pytz

from argparse import ArgumentParser
from datetime import datetime
from functools import reduce
from itertools import islice
from typing import Any, Dict, Callable, Union

from django.core.management.base import CommandError
from django.db import transaction
from django.db.models import Manager, Value, Func, F, QuerySet
from orjson import orjson

from zerver.lib.export import Path
from zerver.lib.management import ZulipBaseCommand
from zerver.models import Realm, Message


class Command(ZulipBaseCommand):
    help = """"""

    def add_arguments(self, parser: ArgumentParser) -> None:
        self.add_realm_args(parser, required=True)
        parser.add_argument(
            "--export_path",
            action="store",
            help="",
            required=True
        )
        parser.add_argument(
            "--old_realm_host",
            action="store",
            help="",
            required=True
        )

    def handle(self, *args: Any, **options: Any) -> None:
        realm = self.get_realm(options)
        assert realm is not None

        import_dir = os.path.realpath(os.path.expanduser(options["export_path"]))
        if not os.path.exists(import_dir):
            raise CommandError(f"Directory not found: '{import_dir}'")
        if not os.path.isdir(import_dir):
            raise CommandError("Export file should be folder; if it's a tarball, please unpack it first.")

        realm_json_path = os.path.join(import_dir, "realm.json")
        if not os.path.exists(realm_json_path):
            raise CommandError(f"File not found: '{realm_json_path}'")
        if not os.path.isfile(realm_json_path):
            raise CommandError("Export file should be folder; if it's a tarball, please unpack it first.")
        if not os.path.exists(realm_json_path):
            raise Exception("Missing realm.json file!")

        logging.info("Processing %s", realm_json_path)
        with open(realm_json_path, "rb") as f:
            realm_json = orjson.loads(f.read())

        old_realm_host = options["old_realm_host"]
        escaped_old_realm_host = re.escape(old_realm_host)

        update_user_email(realm, old_realm_host)

        exported_stream_id_key_by_current = join_exported_with_current_id(
            "date_created", realm_json["zerver_stream"], realm.stream_set)
        logging.info("Mapped %s stream(s) id", len(exported_stream_id_key_by_current))

        replace_exported_with_current_in_message(
            realm.message_set,
            "#narrow/stream_id",
            exported_stream_id_key_by_current,
            lambda exported: fr"https:\/\/{escaped_old_realm_host}\/#narrow\/stream\/{exported}",
            lambda current: fr"https://{realm.host}/#narrow/stream/{current}")

        exported_user_id_key_by_current = join_exported_with_current_id(
            "date_joined", realm_json["zerver_userprofile"], realm.userprofile_set)
        logging.info("Mapped %s user(s) id", len(exported_user_id_key_by_current))
        replace_exported_with_current_in_message(
            realm.message_set,
            "@user mentions",
            exported_user_id_key_by_current,
            lambda exported: fr"@(_?)\*\*(.+)\|{exported}\*\*",
            lambda current: fr"@\1**\2|{current}**")

        replace_exported_with_current_in_message(
            realm.message_set,
            "#narrow/near/msg_id",
            guard_only_unique_id(mapping_message_id(realm, import_dir)),
            lambda exported: fr"https:\/\/{escaped_old_realm_host}\/#narrow\/(.+)near\/{exported}",
            lambda current: fr"https://{realm.host}/#narrow/\1near/{current}")


def guard_only_unique_id(exported_id_key_by_current: Dict[int, int]) -> dict[int, int]:
    duplicate_id_count = len(set(exported_id_key_by_current.keys())
                             .intersection(exported_id_key_by_current.values()))
    if duplicate_id_count != 0:
        raise KeyError("%s duplicate id has been found", duplicate_id_count)
    return exported_id_key_by_current


def update_user_email(realm: Realm, old_realm_host: str) -> None:
    users_with_email_at_old_realm_host = realm.userprofile_set.filter(email__endswith=old_realm_host)
    escaped_old_realm_host = re.escape(old_realm_host)
    user_id_at_host = re.compile(fr"^user\d+@{escaped_old_realm_host}$")
    email_host_part = re.compile(fr"@{escaped_old_realm_host}$")

    for user in users_with_email_at_old_realm_host:
        if user_id_at_host.match(user.email):
            user.email = "user%s@%s" % (user.id, realm.host)
        elif user.is_bot:
            user.email = email_host_part.sub(f"@{realm.host}", user.email)
        user.save()

    logging.info("Updated %s user(s) id and realm host from %s to %s in their 'zerver_userprofile.email'",
                 len(users_with_email_at_old_realm_host), old_realm_host, realm.host)


def replace_exported_with_current_in_message(
    message_query_set: QuerySet[Message],
    replace_type_in_log: str,
    exported_id_key_by_current: Dict[int, int],
    get_pattern: Callable[[str], str],
    get_replacement: Callable[[str], str]
) -> None:
    # https://realpython.com/how-to-split-a-python-list-into-chunks/#custom-implementation-of-batched
    def batched(iterable, chunk_size):
        iterator = iter(iterable)
        return iter(
            lambda: tuple(islice(iterator, chunk_size)),
            tuple()
        )

    def join_to_regex_alternate(values: list) -> str:
        return f"({'|'.join(map(str, values))})"

    def get_function_expression(source: Union[Func, F], current_and_exported: (int, int)):
        return Func(source,
                    Value(get_pattern(current_and_exported[1])),
                    Value(get_replacement(current_and_exported[0])),
                    function="regexp_replace")

    # https://stackoverflow.com/questions/3323001/what-is-the-maximum-recursion-depth-and-how-to-increase-it
    sys.setrecursionlimit(15000)
    for current_and_exported_chunk in batched(exported_id_key_by_current.items(), 3000):
        exported = [i[1] for i in current_and_exported_chunk]
        try:
            with transaction.atomic():
                recursion_start = time.perf_counter()
                content_expression = reduce(
                    lambda acc_expr, current_and_exported:
                    get_function_expression(acc_expr, current_and_exported),
                    current_and_exported_chunk[1:],
                    get_function_expression(F("content"), current_and_exported_chunk[0]))
                recursion_end = time.perf_counter()
                update_start = time.perf_counter()
                updated_rows = message_query_set.filter(
                    content__regex=get_pattern(join_to_regex_alternate(exported))
                ).update(
                    content=content_expression,
                    rendered_content_version=0
                )
                update_end = time.perf_counter()
                logging.info("Spend %ss in django and %ss in pgsql to "
                             "replace %s exported id in %s affects %s message(s)",
                             str(round(recursion_end - recursion_start, 3)).ljust(7),
                             str(round(update_end - update_start, 3)).ljust(7),
                             len(current_and_exported_chunk), replace_type_in_log, updated_rows)
                raise InterruptedError
        except InterruptedError:
            pass


def mapping_message_id(realm: Realm, import_dir: Path) -> Dict[int, int]:
    exported_message_id_key_by_current: Dict[int, int] = {}
    dump_file_id = 1
    while True:
        message_json_path = os.path.join(import_dir, f"messages-{dump_file_id:06}.json")
        if not os.path.exists(message_json_path):
            break

        logging.info("Mapping messages id in %s", message_json_path)

        with open(message_json_path, "rb") as f:
            message_json = orjson.loads(f.read())["zerver_message"]

        datetime_list: list[float] = [i["date_sent"] for i in message_json]
        datetime_range = (datetime.fromtimestamp(min(datetime_list), tz=pytz.UTC),
                          datetime.fromtimestamp(max(datetime_list), tz=pytz.UTC))
        message_set = realm.message_set.filter(date_sent__range=datetime_range)
        archived_message_set = realm.archivedmessage_set.filter(date_sent__range=datetime_range)

        exported_message_id_key_by_current_in_current_dump = join_exported_with_current_id(
            "date_sent", message_json, message_set.union(archived_message_set))
        if (exported_message_id_key_by_current_in_current_dump.keys()
            & exported_message_id_key_by_current.keys()
            or exported_message_id_key_by_current_in_current_dump.values()
            & exported_message_id_key_by_current.keys()):
            raise KeyError("Conflict message id or date_send timestamp across multiple dumps.")

        exported_message_id_key_by_current.update(exported_message_id_key_by_current_in_current_dump)
        dump_file_id += 1

    return exported_message_id_key_by_current


def join_exported_with_current_id(join_key: str, exported_json: dict,
                                  current_query_set: Manager) -> Dict[int, int]:
    join_key_current_list = current_query_set.values_list(join_key, "id")

    join_key_current_set = {i[0] for i in join_key_current_list}
    join_key_current_list_set_diff = len(join_key_current_list) - len(join_key_current_set)
    if join_key_current_list_set_diff != 0:
        logging.warning("%s record(s) in current database sharing same value of the key '%s'",
                        join_key_current_list_set_diff, join_key)

    join_key_exported_set = {i[join_key] for i in exported_json}
    join_key_exported_set_diff = len(exported_json) - len(join_key_exported_set)
    if join_key_exported_set_diff != 0:
        logging.warning("%s record(s) in exported json sharing same value of the key '%s'",
                        join_key_exported_set_diff, join_key)

    exported: Dict[Any, int] = dict([(i[join_key], i["id"]) for i in exported_json])
    current: Dict[Any, int] = dict([(i[0].timestamp(), i[1]) for i in join_key_current_list])
    exported_keys = exported.keys()
    current_keys = current.keys()

    keys_only_in_exported = exported_keys - current_keys
    if keys_only_in_exported:
        logging.warning("%s record(s) in exported json cannot be mapped with the current database"
                        " by the key '%s'", len(keys_only_in_exported), join_key)

    keys_only_in_current = current_keys - exported_keys
    if keys_only_in_current:
        logging.warning("%s record(s) in current database cannot be mapped with the exported json"
                        " by the key '%s'", len(keys_only_in_current), join_key)

    return guard_only_unique_id({current[k]: exported[k] for k in exported_keys & current_keys})
