#!/usr/bin/env bash
# Owner scoping on a built smitline image, over real HTTP: a server-mode daemon with
# SMITLINE_REQUIRE_OWNER=1, an internal gateway token naming owners with X-Smitline-Owner,
# a token created for one owner, and a hooks module loaded with SMITLINE_CALL_HOOKS whose
# refusal carries a status and details. It places no calls and needs no keys or Docker
# socket. Everything it makes (container, volume) is throwaway and removed at the end.
#
#   scripts/smoke-owners.sh [IMAGE] [HOST_PORT]     (defaults: smitline:test 18665)
set -euo pipefail
IMAGE="${1:-smitline:test}"
PORT="${2:-18665}"
NAME="smitline-smoke-owners-$$"
WORK="$(mktemp -d)"

cleanup() {
  status=$?
  if [[ $status -ne 0 ]]; then
    echo "--- daemon log" >&2
    docker logs "$NAME" 2>&1 | tail -40 >&2 || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker volume rm "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $status
}
trap cleanup EXIT

# Hosted-style hooks: owner acct_poor is refused with 402 and the details a wallet would add.
cat > "$WORK/smoke_hooks.py" <<'PY'
from call_hooks import CallRefused, DefaultCallHooks


class SmokeHooks(DefaultCallHooks):
    def precheck(self, owner, brief):
        if owner == 'acct_poor':
            raise CallRefused('insufficient_credit', 'Top up to call.', status=402,
                              balance='0.40', topUpUrl='https://example.com/top-up')
        return super().precheck(owner, brief)


def create(**kwargs):
    return SmokeHooks(**kwargs)
PY
chmod 644 "$WORK/smoke_hooks.py"

docker volume create "$NAME" >/dev/null
# The data volume belongs to the app user; tokens are made before the daemon starts, because
# server mode refuses to start without one.
docker run --rm -v "$NAME:/data" --entrypoint sh "$IMAGE" -c 'chown -R app:app /data && chmod 700 /data'
token() {
  docker run --rm --user app -v "$NAME:/data" --entrypoint python "$IMAGE" \
    meeting-runtime/api_tokens.py --root /data create "$@" 2>/dev/null \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
}
INTERNAL="$(token --name gateway --kind internal)"
STANDARD="$(token --name script)"
OWNED="$(token --name account-b --owner acct_b)"

docker run -d --name "$NAME" -v "$NAME:/data" -p "127.0.0.1:$PORT:8765" \
  -v "$WORK/smoke_hooks.py:/app/meeting-runtime/smoke_hooks.py:ro" \
  -e SMITLINE_SERVER_MODE=1 -e SMITLINE_DAEMON_HOST=0.0.0.0 -e SMITLINE_REQUIRE_OWNER=1 \
  -e SMITLINE_CALL_HOOKS=smoke_hooks:create "$IMAGE" >/dev/null
for _ in $(seq 1 60); do
  code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/v1/openapi.json" || true)"
  [[ "$code" == 401 ]] && break
  sleep 1
done
[[ "$code" == 401 ]] || { echo "the daemon did not answer on port $PORT" >&2; exit 1; }
LAUNCH="$(docker exec -u app "$NAME" cat /data/.smitline/daemon.auth)"

python3 - "http://127.0.0.1:$PORT" "$LAUNCH" "$INTERNAL" "$STANDARD" "$OWNED" <<'PY'
import json
import sys
import urllib.error
import urllib.request

base, launch, internal, standard, owned = sys.argv[1:]
A, B, POOR = 'acct_a', 'acct_b', 'acct_poor'
failures = []


def call(method, path, token=None, owner=None, body=None):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    if owner is not None:
        headers['X-Smitline-Owner'] = owner
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read() or b'null')
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b'null')


def expect(label, got, status, code=None):
    status_got, payload = got
    code_got = (payload or {}).get('error', {}).get('code') if isinstance(payload, dict) else None
    ok = status_got == status and (code is None or code_got == code)
    print(f"{'ok  ' if ok else 'FAIL'} {label}: {status_got} {code_got or ''}".rstrip())
    if not ok:
        failures.append(f'{label}: wanted {status} {code or ""}, got {status_got} {payload}')
    return payload


def check(label, condition, detail=''):
    print(f"{'ok  ' if condition else 'FAIL'} {label}")
    if not condition:
        failures.append(f'{label} {detail}')


# afterHours: the checks below must not depend on the time of day where the number rings.
phone = lambda to, **extra: {'channel': 'phone', 'to': to, 'onBehalfOf': 'Robin', 'afterHours': True,
                             'objective': 'Book a table for 4 at 7pm', **extra}

# Who may act, and as whom.
expect('no credential', call('GET', '/v1/calls'), 401, 'unauthorized')
expect('per-launch token acts as nobody', call('GET', '/v1/calls', launch), 401, 'owner_required')
expect('per-launch token cannot name an owner', call('GET', '/v1/calls', launch, A), 403, 'owner_header_forbidden')
expect('standard token cannot name an owner', call('GET', '/v1/calls', standard, A), 403, 'owner_header_forbidden')
expect('standard token acts as nobody', call('GET', '/v1/profile', standard), 401, 'owner_required')
expect('internal token must name an owner', call('GET', '/v1/calls', internal), 401, 'owner_required')
expect('internal token naming local', call('GET', '/v1/calls', internal, 'local'), 401, 'owner_required')
expect('owner token cannot name another owner', call('GET', '/v1/calls', owned, A), 403, 'owner_header_forbidden')
for bad in ('../x', 'ACCT', 'a' * 65):
    expect(f'invalid owner {bad[:12]!r}', call('GET', '/v1/calls', internal, bad), 400, 'invalid_owner')
expect('raw meetings are closed (owner)', call('POST', '/v1/meetings', internal, A, {'meetingUrl': 'https://zoom.us/j/123456789'}), 403, 'local_owner_only')
expect('raw meetings are closed (launch)', call('POST', '/v1/meetings', launch, None, {'meetingUrl': 'https://zoom.us/j/123456789'}), 403, 'local_owner_only')
expect('only the launch token stops the daemon', call('POST', '/v1/daemon/stop', internal, A, {}), 403, 'forbidden')

# Owner A's contacts, profile and do-not-call list.
expect('A saves a contact', call('PATCH', '/v1/contacts/%2B14155550101', internal, A, {'name': 'Sam', 'notes': 'A only'}), 200)
expect('A saves a profile', call('PATCH', '/v1/profile', internal, A, {'about': 'Owner A runs a bakery.'}), 200)
expect('A blocks a number', call('PATCH', '/v1/do-not-call', internal, A, {'add': ['+14155550177']}), 200)
for label, token, owner in (('B through the gateway', internal, B), ('B with its own token', owned, None)):
    payload = expect(f'{label}: profile', call('GET', '/v1/profile', token, owner), 200)
    check(f'{label}: sees none of A\'s profile', 'bakery' not in json.dumps(payload))
    payload = expect(f'{label}: contacts', call('GET', '/v1/contacts', token, owner), 200)
    check(f'{label}: sees none of A\'s contacts', payload == {'contacts': []}, payload)
    expect(f'{label}: A\'s contact is not found', call('GET', '/v1/contacts/%2B14155550101', token, owner), 404, 'not_found')
    payload = expect(f'{label}: do-not-call', call('GET', '/v1/do-not-call', token, owner), 200)
    check(f'{label}: A\'s do-not-call list is not B\'s', payload == {'numbers': []}, payload)
payload = expect('A still has its contact', call('GET', '/v1/contacts/%2B14155550101', internal, A), 200)
check('A\'s contact is unchanged', (payload or {}).get('notes') == 'A only', payload)

# Calls: guardrails per owner, refusals with status and details, nothing dialed without keys.
payload = expect('A checks a call to its blocked number', call('POST', '/v1/calls/check', internal, A, phone('+14155550177')), 200)
check('A\'s check names the do-not-call list', any('do-not-call' in p for p in payload['problems']), payload)
payload = expect('B checks the same call', call('POST', '/v1/calls/check', internal, B, phone('+14155550177')), 200)
check('B\'s check does not', not any('do-not-call' in p for p in payload['problems']), payload)
check('B\'s check only lacks keys', payload['problems'] and all('not configured' in p for p in payload['problems']), payload)
expect('A cannot call its blocked number', call('POST', '/v1/calls', internal, A, phone('+14155550177')), 403, 'do_not_call')
expect('B has no keys, so nothing is dialed', call('POST', '/v1/calls', internal, B, phone('+14155550177')), 503, 'not_configured')
expect('an invalid meeting link', call('POST', '/v1/calls', internal, A, {'channel': 'meeting', 'to': 'https://example.com/not-a-meeting', 'onBehalfOf': 'Robin', 'objective': 'Take notes'}), 422)
payload = expect('a hook refusal keeps its status and details', call('POST', '/v1/calls', internal, POOR, phone('+14155550150')), 402, 'insufficient_credit')
check('the refusal carries balance and topUpUrl', (payload or {}).get('error', {}).get('topUpUrl') == 'https://example.com/top-up'
      and payload['error'].get('balance') == '0.40', payload)
payload = expect('the check lists the refusal', call('POST', '/v1/calls/check', internal, POOR, phone('+14155550150')), 200)
check('as a problem', 'Top up to call.' in payload['problems'], payload)
for owner in (A, B, POOR):
    payload = expect(f'{owner} lists its calls', call('GET', '/v1/calls', internal, owner), 200)
    check(f'{owner} has no call records (all refused before dialing)', payload['calls'] == [], payload)

if failures:
    print('\n'.join(['', 'FAILED:'] + failures), file=sys.stderr)
    sys.exit(1)
print(f'\nall owner checks passed against {base}')
PY

# Files: each owner's in its own folder, private; the local owner's untouched.
docker exec -u app "$NAME" sh -c '
  set -e
  cd /data/.smitline
  for file in contacts.json profile.json do-not-call.json; do
    test "$(stat -c %a "owners/acct_a/$file")" = 600 || { echo "owners/acct_a/$file is not 0600"; exit 1; }
  done
  test "$(stat -c %a owners/acct_a)" = 700 || { echo "owners/acct_a is not 0700"; exit 1; }
  for file in contacts.json profile.json do-not-call.json; do
    test ! -e "$file" || { echo "the local owner got $file"; exit 1; }
  done
  test ! -e owners/acct_b/contacts.json || { echo "owner B got a contact book"; exit 1; }
  echo "ok   owner files are private and in their own folders"
'
