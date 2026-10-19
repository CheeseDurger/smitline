"""Long-lived API tokens for server mode. Only SHA-256 digests are stored."""
import old_names
old_names.adopt_old_settings()

from datetime import datetime, timezone
from pathlib import Path
import argparse
import hashlib
import hmac
import json
import os
import secrets
import sys
import threading


TOKEN_PREFIX = 'cai_'
FILE_NAME = 'api-tokens.json'
# A standard token acts for its owner (or the local owner); an internal token, held by a
# hosted gateway, may name the owner of each request with X-Smitline-Owner.
TOKEN_KINDS = ('standard', 'internal')


def _digest(token):
    return hashlib.sha256(token.encode('utf-8')).hexdigest()


def _now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class ApiTokenStore:
    def __init__(self, path):
        self.path = Path(path)
        self._lock = threading.Lock()

    def _read(self):
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
        except FileNotFoundError:
            return []
        entries = data.get('tokens') if isinstance(data, dict) else None
        return [entry for entry in entries or () if isinstance(entry, dict)]

    def _write(self, entries):
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        tmp = self.path.with_name(f'.{self.path.name}.{secrets.token_hex(4)}.tmp')
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            os.write(fd, json.dumps({'version': 1, 'tokens': entries}, indent=2).encode('utf-8'))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.path)

    def create(self, name, *, owner=None, kind='standard'):
        from call_hooks import valid_owner
        name = str(name or '').strip()[:80] or 'unnamed'
        if kind not in TOKEN_KINDS:
            raise ValueError('kind must be standard or internal')
        if owner is not None and not valid_owner(owner):
            raise ValueError('owner must be lowercase letters, digits, _ and -, at most 64 characters')
        token = TOKEN_PREFIX + secrets.token_urlsafe(32)
        entry = {'id': 'tok-' + secrets.token_hex(6), 'name': name, 'createdAt': _now(),
                 'digest': _digest(token)}
        if kind != 'standard':
            entry['kind'] = kind
        if owner is not None:
            entry['owner'] = owner
        with self._lock:
            entries = self._read()
            entries.append(entry)
            self._write(entries)
        return token, self.public(entry)

    @staticmethod
    def public(entry):
        public = {'id': entry['id'], 'name': entry.get('name'), 'createdAt': entry.get('createdAt'),
                  'kind': entry.get('kind') if entry.get('kind') in TOKEN_KINDS else 'standard'}
        if entry.get('owner'):
            public['owner'] = entry['owner']
        return public

    def list(self):
        return [self.public(entry) for entry in self._read()]

    def revoke(self, token_id):
        with self._lock:
            entries = self._read()
            kept = [entry for entry in entries if entry.get('id') != token_id]
            if len(kept) == len(entries):
                return False
            self._write(kept)
            return True

    def verify(self, token):
        if not isinstance(token, str) or not token.startswith(TOKEN_PREFIX):
            return None
        digest = _digest(token)
        match = None
        for entry in self._read():
            if hmac.compare_digest(str(entry.get('digest') or ''), digest):
                match = entry
        return self.public(match) if match else None

    def __len__(self):
        return len(self._read())


def default_path(project_root):
    from runtime_state import daemon_data_path
    return daemon_data_path(project_root) / FILE_NAME


def main(argv=None):
    parser = argparse.ArgumentParser(description='Manage Smitline API tokens')
    parser.add_argument('--root', default=str(Path(__file__).resolve().parent.parent))
    commands = parser.add_subparsers(dest='command', required=True)
    create = commands.add_parser('create', help='create a token and print it once')
    create.add_argument('--name', required=True)
    create.add_argument('--owner', help='the owner id this token always acts for')
    create.add_argument('--kind', choices=TOKEN_KINDS, default='standard',
                        help='internal: may name the owner per request with X-Smitline-Owner')
    commands.add_parser('list', help='list tokens without their values')
    revoke = commands.add_parser('revoke', help='revoke a token by id')
    revoke.add_argument('id')
    args = parser.parse_args(argv)
    store = ApiTokenStore(default_path(Path(args.root).resolve()))
    if args.command == 'create':
        try:
            token, entry = store.create(args.name, owner=args.owner, kind=args.kind)
        except ValueError as error:
            print(str(error), file=sys.stderr)
            return 2
        print(json.dumps({'token': token, **entry}, indent=2))
        print('Store this token now; it is not shown again.', file=sys.stderr)
    elif args.command == 'list':
        print(json.dumps(store.list(), indent=2))
    else:
        if not store.revoke(args.id):
            print('No token with that id.', file=sys.stderr)
            return 1
        print('Revoked.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
