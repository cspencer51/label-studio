import json
import logging
import os
import sqlite3

from label_studio.utils.io import iter_files
from .base import BaseStorage, StringField, BooleanField, Optional

logger = logging.getLogger(__name__)


class Sqlite3Storage(BaseStorage):

    description = "Sqlite3 task database"

    def __init__(self, **kwargs):
        super(Sqlite3Storage, self).__init__(**kwargs)

        conn = sqlite3.connect(self.path)
        self.create_table()
        conn.close()

    @property
    def readable_path(self):
        return self.path

    def get(self, id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT data, posting_id FROM tasks WHERE id=?''', (id,))
        ret = c.fetchone()
        conn.close()

        if ret is None:
            return ret
        else:
            data = {"id": id, "data": {"text": ret[0], "posting_id": ret[1]}}

        return data

    def set(self, id, value):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''INSERT INTO tasks (id, data, posting_id) VALUES (?,?,?)''', (id, value['text'], value['posting_id']))
        conn.commit()
        conn.close()

    def __contains__(self, id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT * FROM tasks WHERE id=?''', (id,))
        ret = c.fetchone() is not None
        conn.close()
        return ret

    # used by upload API
    def set_many(self, ids, values):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()

        data = [(i, v['data']['text'], v['data']['posting_id']) for i, v in zip(ids, values)]
        c.executemany('''INSERT OR REPLACE INTO tasks (id, data, posting_id)
            values (?,?,?)''', data)

        conn.commit()
        conn.close()

    def ids(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''SELECT id FROM tasks''')
        ids = [i[0] for i in c.fetchall()]
        conn.close()
        return ids

    def max_id(self):
        return max(self.ids(), default=-1)

    def items(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT id, data, posting_id FROM tasks''')
        data = {x[0]: {'id': x[0], 'data': {'text': x[1], 'posting_id': x[2]}} for x in c.fetchall()}
        conn.close()
        return data.items()

    def remove(self, key):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''DELETE FROM tasks where id=?''', (key,))
        conn.commit()
        conn.close()

    def remove_all(self):
        self.drop_table()
        self.create_table()

    def empty(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM tasks''')
        ret = c.fetchone() == 0
        conn.close()
        return ret

    def sync(self):
        pass

    def create_table(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS tasks (id integer, data text, posting_id text, PRIMARY KEY (id))''')
        conn.commit()
        conn.close()

    def drop_table(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''DROP TABLE tasks''')
        conn.commit()
        conn.close()

    def get_schema(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''PRAGMA table_info(tasks)''')
        ret = [x[1] for x in c.fetchall()]
        conn.close()
        return ret


def already_exists_error(what, path):
    raise RuntimeError('{path} {what} already exists. Use "--force" option to recreate it.'.format(
        path=path, what=what))


class Sqlite3CompletionsStorage(BaseStorage):

    description = "Sqlite3 completions database"

    def __init__(self, **kwargs):
        super(Sqlite3CompletionsStorage, self).__init__(**kwargs)

        conn = sqlite3.connect(self.path)
        self.create_table()
        conn.close()

    @property
    def readable_path(self):
        return self.path

    def get(self, id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT task_data, completion_id, result_id, lead_time,
            choice, from_name, to_name, type, created_at, posting_id, was_cancelled FROM completions WHERE task_id=?''', (id,))
        ret = c.fetchone()
        conn.close()

        if ret is None:
            return ret
        else:
            data = {
                "id": id,
                "data": {
                    "text": ret[0],
                    "posting_id": ret[9]
                },
                "completions": [{
                      "created_at": ret[8],
                      "id": ret[1],
                      "lead_time": ret[3],
                      "result": []
                    }]
            }

            if ret[10] == "1":
                data["completions"][0]["was_cancelled"] = True
            else:
                data["completions"][0]["result"].append({
                            "value": {
                                "choices": [
                                    ret[4]
                                ]
                            },
                            "id": ret[2],
                            "from_name": ret[5],
                            "to_name": ret[6],
                            "type": ret[7]
                        })

            return data

    def set(self, id, value):
        task_id = value['id']
        task_data = value['data']['text']
        posting_id = value['data']['posting_id']

        completions = value['completions'][-1]
        lead_time = completions['lead_time']
        completion_id = completions['id']
        created_at = completions['created_at']
        was_cancelled = completions.get("was_cancelled", False)

        result_id = ""
        choice = ""
        choice_id = ""
        from_name = ""
        to_name = ""
        choice_type = ""

        result = completions['result']
        if len(result) >= 1:
            result = result[-1]
            result_id = result['id']
            choice = result['value']['choices'][0]
            choice_id = result['type']
            from_name = result['from_name']
            to_name = result['to_name']
            choice_type = result['type']

        conn = sqlite3.connect(self.path)
        c = conn.cursor()

        if self.get(id) is not None:
            c.execute('''UPDATE completions
                SET task_data = ?,
                    result_id = ?,
                    lead_time = ?,
                    choice = ?,
                    choice_id = ?,
                    from_name = ?,
                    to_name = ?,
                    type = ?,
                    created_at = ?,
                    posting_id = ?,
                    was_cancelled = ?
                WHERE task_id = ? AND completion_id = ?''',
                    (task_data, result_id, lead_time, choice, choice_id,
                     from_name, to_name, choice_type, created_at, posting_id,
                     was_cancelled, task_id, completion_id))
        else:
            c.execute('''INSERT INTO completions (task_id, task_data, completion_id,
                result_id, lead_time, choice, choice_id, from_name, to_name, type, created_at,
                posting_id, was_cancelled)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (task_id, task_data, completion_id, result_id,
                    lead_time, choice, choice_id, from_name, to_name, choice_type, created_at,
                    posting_id, was_cancelled))

        conn.commit()
        conn.close()

    def __contains__(self, id):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT * FROM completions WHERE task_id=?''', (id,))
        ret = c.fetchone() is not None
        conn.close()
        return ret

    # used by upload API
    def set_many(self, ids, values):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        data = [(i, v['data']['text']) for i, v in zip(ids, values)]
        c.executemany('''INSERT OR REPLACE INTO completions (task_id, task_data, completion_id,
            result_id, lead_time, choice, choice_id, from_name, to_name, type, created_at,
            posting_id, was_cancelled)
            values (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)

        conn.commit()
        conn.close()

    def ids(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''SELECT DISTINCT task_id FROM completions''')
        ids = [i[0] for i in c.fetchall()]
        conn.close()
        return ids

    def max_id(self):
        return max(self.ids(), default=-1)

    def items(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''
            SELECT task_id,
                task_data,
                completion_id,
                result_id,
                lead_time,
                choice,
                choice_id,
                from_name,
                to_name,
                type,
                created_at,
                posting_id,
                was_cancelled
            FROM completions''')

        data = {}
        for x in c.fetchall():
            task_id = x[0]
            task_data = x[1]
            completion_id = x[2]
            result_id = x[3]
            lead_time = x[4]
            choice = x[5]
            choice_id = x[6]
            from_name = x[7]
            to_name = x[8]
            choice_type = x[9]
            created_at = x[10]
            posting_id = x[11]
            was_cancelled = x[12]

            completion = {
                "lead_time": lead_time,
                "result": [
                    {
                        "value": {
                            "choices": [
                                choice
                            ]
                        },
                        "id": choice_id,
                        "from_name": from_name,
                        "to_name": to_name,
                        "type": choice_type
                    }
                ],
                "id": result_id,
                "created_at": created_at
            }

            if was_cancelled == "1":
                completion["was_cancelled"] = True

            if task_id in data:
                data[task_id]["completions"].append(completion)

            else:
                data[task_id] = {
                    "id": task_id,
                    "data": {
                        "text": task_data,
                        "posting_id": posting_id
                    },
                    "completions": [completion],
                }

        conn.close()
        return data.items()

    def remove(self, key):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''DELETE FROM completions where task_id=?''', (key,))
        conn.commit()
        conn.close()

    def remove_all(self):
        self.drop_table()
        self.create_table()

    def empty(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''SELECT COUNT(*) FROM completions''')
        ret = c.fetchone() == 0
        conn.close()
        return ret

    def sync(self):
        pass

    def create_table(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS completions
            (task_id integer,
             task_data text,
             completion_id integer,
             result_id integer,
             lead_time integer,
             choice text,
             choice_id text,
             from_name text,
             to_name text,
             type text,
             created_at integer,
             posting_id text,
             was_cancelled text,
             PRIMARY KEY (task_id, completion_id, result_id)
            )''')
        conn.commit()
        conn.close()

    def drop_table(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''DROP TABLE completions''')
        conn.commit()
        conn.close()

    def get_schema(self):
        conn = sqlite3.connect(self.path)
        c = conn.cursor()
        c.execute('''PRAGMA table_info(completions)''')
        ret = [x[1] for x in c.fetchall()]
        conn.close()
        return ret
