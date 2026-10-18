"""Several meetings at once: capacity, ports, mounts, claims, and restarts."""
import asyncio
import json
import os
import tempfile
import unittest
from pathlib import Path

from agent_sessions import MeetingSession
from daemon_main import build_supervisor
from meeting_supervisor import (
    CODE_ROOT, CommandResult, ComposeMeetingAgent, DaemonError, MeetingPool,
    ProductionMeetingSupervisor, compose_command, pool_settings,
)
from runtime_state import active_meeting_path, file_mode, pool_claim_path, read_json, write_private_json
from test_meeting_supervisor import FakeDaemon, FakeHealth, FakeLauncher, FakeRecord, session


def owned(meeting_id, owner):
    return MeetingSession.from_dict(dict(session(id=meeting_id).to_dict(), owner=owner))


async def until(predicate, timeout=3.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError('condition not met in time')
        await asyncio.sleep(0.005)


class SlowLauncher(FakeLauncher):
    """compose up and down that take their time, so starts and stops overlap. Records the
    owner env file as compose would read it, while it exists."""

    def __init__(self, up_gate=None, stop_gate=None, fail_up=False):
        super().__init__()
        self.up_gate, self.stop_gate, self.fail_up = up_gate, stop_gate, fail_up
        self.stopping = False
        self.env_file = None

    async def up(self, env):
        path = env.get('SMITLINE_MEETING_ENV_FILE')
        if path:
            self.env_file = (path, Path(path).read_text(), file_mode(Path(path)))
        if self.up_gate is not None:
            await self.up_gate.wait()
        await super().up(env)

    async def stop(self):
        self.stopping = True
        if self.stop_gate is not None:
            await self.stop_gate.wait()
        await super().stop()


def pair(launcher):
    return int(launcher.env['SMITLINE_MEETING_VNC_PORT']), int(launcher.env['SMITLINE_MEETING_HEALTH_PORT'])


class ComposeRunner:
    """Answers `docker compose ls` with the given projects and records every command."""

    def __init__(self, projects=()):
        self.projects = list(projects)
        self.calls = []

    async def run(self, args, *, env=None):
        self.calls.append(list(args))
        if args[:3] == ['docker', 'compose', 'ls']:
            return CommandResult(0, json.dumps(self.projects), '')
        return CommandResult(0, '', '')


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.runtime = self.root / 'meeting-runtime'
        self.runtime.mkdir()
        self.launchers = {}
        self.busy = set()
        self.runner = ComposeRunner()
        self.daemon = FakeDaemon()
        self.pool = self.make_pool()

    def make_pool(self, **overrides):
        def launcher(project, env):
            fake = FakeLauncher()
            fake.project, fake.env = project, env
            self.launchers[project] = fake
            return fake
        options = dict(runtime_root=self.runtime, runner=self.runner, environ={}, max_meetings=2,
                       ports=(18100, 18109), poll_interval=0.02, start_timeout=0.4,
                       stop_timeout=0.4, launcher_factory=launcher,
                       health_factory=lambda port: FakeHealth(),
                       port_free=lambda port: port not in self.busy)
        options.update(overrides)
        return MeetingPool(self.root, **options).bind_daemon(self.daemon)

    async def asyncTearDown(self):
        await self.pool.shutdown()
        self.temporary.cleanup()

    def slow(self, up_gate=None, stop_gate=None, fail_up=False):
        def launcher(project, env):
            fake = SlowLauncher(up_gate, stop_gate, fail_up)
            fake.project, fake.env = project, env
            self.launchers[project] = fake
            return fake
        return launcher

    async def test_meetings_started_at_the_same_time_never_share_ports(self):
        gate = asyncio.Event()
        self.pool = self.make_pool(max_meetings=6, ports=(18100, 18199), launcher_factory=self.slow(gate))
        self.busy = {18102, 18105}
        meetings = [session(id=f'mtg-race{index:012x}') for index in range(6)]
        starts = [asyncio.create_task(self.pool.start(meeting)) for meeting in meetings]
        # Every meeting has its ports while every compose up is still running.
        await until(lambda: len(self.launchers) == 6)
        gate.set()
        await asyncio.gather(*starts)
        pairs = [pair(launcher) for launcher in self.launchers.values()]
        ports = [port for both in pairs for port in both]
        self.assertEqual(len(set(ports)), 12, pairs)
        self.assertFalse(set(ports) & self.busy)
        claimed = [tuple(read_json(pool_claim_path(self.root, meeting.id))['ports'].values()) for meeting in meetings]
        self.assertEqual(sorted(claimed), sorted(pairs))

    async def test_ports_and_capacity_stay_taken_until_the_container_is_gone(self):
        stop_gate = asyncio.Event()
        self.pool = self.make_pool(max_meetings=2, ports=(18100, 18103),
                                   launcher_factory=self.slow(stop_gate=stop_gate))
        first, second, third = (session(id='mtg-first00000001'), session(id='mtg-second0000001'),
                                session(id='mtg-third00000001'))
        await self.pool.start(first)
        first_launcher = self.launchers['smitline-meeting-' + first.id]
        canceling = asyncio.create_task(self.pool.cancel(first.id))
        await until(lambda: first_launcher.stopping)
        # While the first container is still being removed, its ports and its place are taken.
        await self.pool.start(second)
        self.assertNotEqual(pair(self.launchers['smitline-meeting-' + second.id]), pair(first_launcher))
        with self.assertRaises(DaemonError) as raised:
            await self.pool.start(third)
        self.assertEqual(raised.exception.code, 'capacity_exceeded')
        stop_gate.set()
        await canceling
        await self.pool.start(third)
        self.assertEqual(pair(self.launchers['smitline-meeting-' + third.id]), pair(first_launcher))

    async def test_a_meeting_that_ends_by_itself_frees_its_ports_after_its_container(self):
        stop_gate = asyncio.Event()
        health = {}

        def health_for(port):
            health[port] = FakeHealth()
            return health[port]
        self.pool = self.make_pool(max_meetings=2, ports=(18100, 18103), health_factory=health_for,
                                   launcher_factory=self.slow(stop_gate=stop_gate))
        first, second, third = (session(id='mtg-first00000001'), session(id='mtg-second0000001'),
                                session(id='mtg-third00000001'))
        self.daemon.meetings.records[first.id] = FakeRecord(first)
        await self.pool.start(first)
        first_launcher = self.launchers['smitline-meeting-' + first.id]
        health[pair(first_launcher)[1]].payload = {'stage': 'finished'}
        await until(lambda: first_launcher.stopping)
        await self.pool.start(second)
        self.assertNotEqual(pair(self.launchers['smitline-meeting-' + second.id]), pair(first_launcher))
        stop_gate.set()
        await until(lambda: self.pool.active_ids() == (second.id,))
        self.assertFalse(pool_claim_path(self.root, first.id).exists())
        await self.pool.start(third)
        self.assertEqual(pair(self.launchers['smitline-meeting-' + third.id]), pair(first_launcher))

    async def test_the_owner_cap_holds_when_an_owner_starts_two_at_once(self):
        gate = asyncio.Event()
        self.pool = self.make_pool(max_meetings=4, per_owner=1, launcher_factory=self.slow(gate))
        meetings = [owned('mtg-owner0000000a', 'acct_a'), owned('mtg-owner0000000b', 'acct_a')]
        starts = [asyncio.create_task(self.pool.start(meeting)) for meeting in meetings]
        await until(lambda: any(task.done() for task in starts))
        gate.set()
        results = await asyncio.gather(*starts, return_exceptions=True)
        refused = [result for result in results if isinstance(result, DaemonError)]
        self.assertEqual([error.code for error in refused], ['owner_capacity_exceeded'])
        self.assertEqual(len(self.launchers), 1)
        self.assertEqual(len(self.pool.active_ids()), 1)
        self.assertEqual(sum(pool_claim_path(self.root, meeting.id).exists() for meeting in meetings), 1)

    async def test_each_owner_key_reaches_only_its_own_meeting(self):
        gate = asyncio.Event()
        keys = {'acct_a': 'sk-proj-aaaa', 'acct_b': 'sk-proj-bbbb'}
        self.pool = self.make_pool(max_meetings=3, launcher_factory=self.slow(gate),
                                   owner_environment=lambda owner: {
                                       'OPENAI_API_KEY': keys[owner], 'SMITLINE_OWNER_NAME': owner.upper()})
        meetings = [owned('mtg-owner0000000a', 'acct_a'), owned('mtg-owner0000000b', 'acct_b'),
                    session(id='mtg-local00000001')]
        starts = [asyncio.create_task(self.pool.start(meeting)) for meeting in meetings]
        await until(lambda: len(self.launchers) == 3 and all(
            launcher.env_file or launcher.project.endswith('local00000001') for launcher in self.launchers.values()))
        gate.set()
        await asyncio.gather(*starts)
        for owner, meeting in (('acct_a', meetings[0]), ('acct_b', meetings[1])):
            path, body, mode = self.launchers['smitline-meeting-' + meeting.id].env_file
            self.assertEqual(mode, 0o600)
            self.assertIn(f'OPENAI_API_KEY="{keys[owner]}"', body)
            self.assertNotIn(keys['acct_b' if owner == 'acct_a' else 'acct_a'], body)
            self.assertIn(f'SMITLINE_OWNER_NAME="{owner.upper()}"', body)
            # The file sits in the meeting's own folder and is gone once compose read it.
            self.assertEqual(Path(path).parent.name, meeting.id)
            self.assertFalse(Path(path).exists())
        self.assertIsNone(self.launchers['smitline-meeting-mtg-local00000001'].env_file)
        self.assertEqual(list(self.root.rglob('owner.env')), [])

    async def test_the_key_file_is_removed_when_compose_fails(self):
        self.pool = self.make_pool(launcher_factory=self.slow(fail_up=True),
                                   owner_environment=lambda owner: {'OPENAI_API_KEY': 'sk-proj-secret'})
        meeting = owned('mtg-owner0000000a', 'acct_a')
        with self.assertRaises(RuntimeError):
            await self.pool.start(meeting)
        path, body, mode = self.launchers['smitline-meeting-' + meeting.id].env_file
        self.assertEqual(mode, 0o600)
        self.assertIn('sk-proj-secret', body)
        self.assertFalse(Path(path).exists())
        self.assertEqual(list(self.root.rglob('owner.env')), [])
        self.assertEqual(self.pool.active_ids(), ())
        self.assertFalse(pool_claim_path(self.root, meeting.id).exists())

    async def test_the_real_port_probe_skips_a_port_in_use(self):
        import socket
        from meeting_supervisor import port_is_free
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
            held.bind(('127.0.0.1', 0))
            held.listen()
            port = held.getsockname()[1]
            self.assertFalse(port_is_free(port))
            self.pool = self.make_pool(ports=(port, port + 1), port_free=port_is_free)
            with self.assertRaises(DaemonError) as raised:
                await self.pool.start(session())
            self.assertEqual(raised.exception.code, 'no_free_ports')
        self.assertTrue(port_is_free(port))
        await self.pool.start(session())
        self.assertEqual(pair(self.launchers['smitline-meeting-' + session().id]), (port, port + 1))

    async def test_each_meeting_gets_its_own_project_ports_and_claim(self):
        first, second = session(id='mtg-first00000001'), session(id='mtg-second0000001')
        self.busy = {18100}
        await asyncio.gather(self.pool.start(first), self.pool.start(second))
        envs = {launcher.project: launcher.env for launcher in self.launchers.values()}
        self.assertEqual(set(envs), {'smitline-meeting-mtg-first00000001',
                                     'smitline-meeting-mtg-second0000001'})
        ports = sorted((int(env['SMITLINE_MEETING_VNC_PORT']), int(env['SMITLINE_MEETING_HEALTH_PORT']))
                       for env in envs.values())
        # 18100 is taken on the host, so its pair is skipped.
        self.assertEqual(ports, [(18102, 18103), (18104, 18105)])
        claim = read_json(pool_claim_path(self.root, first.id))
        self.assertEqual(claim['meetingId'], first.id)
        self.assertEqual(claim['project'], 'smitline-meeting-mtg-first00000001')
        self.assertEqual(set(claim['ports']), {'vnc', 'health'})
        self.assertFalse(active_meeting_path(self.root).exists())

        await self.pool.cancel(first.id)
        self.assertFalse(pool_claim_path(self.root, first.id).exists())
        self.assertEqual(self.pool.active_ids(), (second.id,))
        # Its ports are free again for the next meeting.
        third = session(id='mtg-third00000001')
        await self.pool.start(third)
        self.assertEqual(self.launchers['smitline-meeting-mtg-third00000001'].env['SMITLINE_MEETING_VNC_PORT'],
                         envs['smitline-meeting-' + first.id]['SMITLINE_MEETING_VNC_PORT'])

    async def test_capacity_and_the_per_owner_cap(self):
        self.pool = self.make_pool(max_meetings=3, per_owner=1)
        await self.pool.start(owned('mtg-owner0000000a', 'acct_a'))
        with self.assertRaises(DaemonError) as raised:
            await self.pool.start(owned('mtg-owner0000000b', 'acct_a'))
        self.assertEqual((raised.exception.status, raised.exception.code), (409, 'owner_capacity_exceeded'))
        # The local owner is never held to the per-owner cap, only to the pool's size.
        await self.pool.start(session(id='mtg-local00000001'))
        await self.pool.start(session(id='mtg-local00000002'))
        with self.assertRaises(DaemonError) as raised:
            await self.pool.start(owned('mtg-owner0000000c', 'acct_c'))
        self.assertEqual(raised.exception.code, 'capacity_exceeded')
        self.assertEqual(len(self.launchers), 3)
        # Starting a running meeting again is not a second meeting.
        await self.pool.start(session(id='mtg-local00000001'))
        self.assertEqual(len(self.launchers), 3)

    async def test_no_free_ports(self):
        self.busy = set(range(18100, 18110))
        with self.assertRaises(DaemonError) as raised:
            await self.pool.start(session())
        self.assertEqual(raised.exception.code, 'no_free_ports')
        self.assertEqual(self.pool.active_ids(), ())

    async def test_a_failed_start_frees_its_place(self):
        def failing(project, env):
            fake = FakeLauncher()
            fake.fail_up = True
            return fake
        self.pool = self.make_pool(launcher_factory=failing)
        with self.assertRaises(RuntimeError):
            await self.pool.start(session())
        self.assertEqual(self.pool.active_ids(), ())
        self.assertFalse(pool_claim_path(self.root, session().id).exists())

    async def test_meetings_mount_only_their_own_folders(self):
        await self.pool.start(session(id='mtg-local00000001'))
        await self.pool.start(owned('mtg-owner0000000a', 'acct_a'))
        local = self.launchers['smitline-meeting-mtg-local00000001'].env
        other = self.launchers['smitline-meeting-mtg-owner0000000a'].env
        self.assertEqual(local['SMITLINE_MEETING_RUN_DIR'],
                         str(self.runtime / 'run' / 'meetings' / 'mtg-local00000001'))
        self.assertEqual(local['SMITLINE_MEETING_RECORDINGS_DIR'],
                         str(self.runtime / 'recordings' / 'mtg-local00000001'))
        self.assertEqual(local['SMITLINE_MEETING_PROFILES_DIR'], str(self.runtime / 'profiles'))
        # Another owner never gets the installation's signed-in profiles, only its own folder.
        self.assertEqual(other['SMITLINE_MEETING_PROFILES_DIR'],
                         str(self.root / '.smitline' / 'owners' / 'acct_a' / 'meeting-profiles'))
        self.assertEqual(other['SMITLINE_MEETING_RECORDINGS_DIR'],
                         str(self.runtime / 'recordings' / 'mtg-owner0000000a'))
        for env in (local, other):
            for key in ('SMITLINE_MEETING_RUN_DIR', 'SMITLINE_MEETING_RECORDINGS_DIR',
                        'SMITLINE_MEETING_PROFILES_DIR'):
                self.assertEqual(file_mode(Path(env[key])), 0o700, key)

    def test_the_published_image_mounts_volume_subpaths(self):
        pool = self.make_pool(environ={'SMITLINE_MEETING_IMAGE': 'ghcr.io/kaelorlabs/smitline-meeting:1'})
        env = pool._mount_env('mtg-owner0000000a', 'acct_a', (18100, 18101))
        self.assertEqual(env['SMITLINE_MEETING_RUN_DIR'], 'meeting-runtime/run/meetings/mtg-owner0000000a')
        self.assertEqual(env['SMITLINE_MEETING_RECORDINGS_DIR'], 'meeting-runtime/recordings/mtg-owner0000000a')
        self.assertEqual(env['SMITLINE_MEETING_PROFILES_DIR'], '.smitline/owners/acct_a/meeting-profiles')
        with tempfile.TemporaryDirectory() as elsewhere:
            outside = self.make_pool(environ={'SMITLINE_MEETING_IMAGE': 'x'}, runtime_root=elsewhere)
            with self.assertRaises(RuntimeError):
                outside._mount_env('mtg-owner0000000a', 'acct_a', (18100, 18101))

    async def test_reconcile_follows_live_meetings_and_removes_only_its_own_other_containers(self):
        live, gone, unclaimed = (session(id='mtg-live000000001'), session(id='mtg-gone000000001'),
                                 session(id='mtg-unclaimed0001'))
        for meeting in (live, gone, unclaimed):
            self.daemon.meetings.records[meeting.id] = FakeRecord(meeting)
        write_private_json(pool_claim_path(self.root, live.id), {
            'meetingId': live.id, 'owner': 'local', 'project': 'smitline-meeting-' + live.id,
            'ports': {'vnc': 18104, 'health': 18105}})
        write_private_json(pool_claim_path(self.root, gone.id), {
            'meetingId': gone.id, 'ports': {'vnc': 18106, 'health': 18107}})
        # A claim of this installation whose meeting record is gone: still this installation's.
        write_private_json(pool_claim_path(self.root, 'mtg-orphan0000001'), {
            'meetingId': 'mtg-orphan0000001', 'ports': {'vnc': 18108, 'health': 18109}})
        self.runner.projects = [
            {'Name': 'smitline-meeting-' + live.id, 'Status': 'running(1)'},
            {'Name': 'smitline-meeting-' + gone.id, 'Status': 'exited(0)'},
            {'Name': 'smitline-meeting-' + unclaimed.id, 'Status': 'running(1)'},
            {'Name': 'smitline-meeting-mtg-orphan0000001', 'Status': 'running(1)'},
            # Another installation's meetings on the same Docker host (a test copy, a hosted
            # deployment beside a personal one) are never this daemon's to remove.
            {'Name': 'smitline-meeting-mtg-elsewhere00001', 'Status': 'running(1)'},
            {'Name': 'smitline-meeting-not-a-meeting', 'Status': 'running(1)'},
            {'Name': 'smitline', 'Status': 'running(1)'},
        ]
        self.pool = self.make_pool()
        await self.pool.reconcile(self.daemon)
        downs = [call for call in self.runner.calls if 'down' in call]
        self.assertEqual(sorted(call[3] for call in downs),
                         ['smitline-meeting-' + gone.id, 'smitline-meeting-mtg-orphan0000001',
                          'smitline-meeting-' + unclaimed.id])
        self.assertFalse(pool_claim_path(self.root, 'mtg-orphan0000001').exists())
        # The single meeting container's project is never touched.
        self.assertFalse(any('smitline' == call[3] for call in downs if len(call) > 3))
        self.assertEqual(self.pool.active_ids(), (live.id,))
        launcher = self.launchers['smitline-meeting-' + live.id]
        self.assertEqual(launcher.env['SMITLINE_MEETING_HEALTH_PORT'], '18105')
        self.assertFalse(pool_claim_path(self.root, gone.id).exists())
        self.assertTrue(pool_claim_path(self.root, live.id).exists())
        # Meetings whose container is gone get their handoff; the live one keeps going.
        finished = {handoff.meeting_id for handoff in self.daemon.handoffs}
        self.assertEqual(finished, {gone.id, unclaimed.id})
        # The adopted meeting's ports are not handed out again.
        launcher.running = True
        await self.pool.start(session(id='mtg-another000001'))
        self.assertNotEqual(self.launchers['smitline-meeting-mtg-another000001'].env['SMITLINE_MEETING_VNC_PORT'],
                            '18104')

    async def test_reconcile_waits_when_docker_does_not_answer(self):
        class Broken(ComposeRunner):
            async def run(self, args, *, env=None):
                return CommandResult(1, '', 'Cannot connect to the Docker daemon')
        meeting = session()
        self.daemon.meetings.records[meeting.id] = FakeRecord(meeting)
        self.pool = self.make_pool(runner=Broken())
        await self.pool.reconcile(self.daemon)
        self.assertEqual(self.daemon.handoffs, [])

    async def test_context_and_cancel_of_a_meeting_without_a_container(self):
        meeting = session()
        self.daemon.meetings.records[meeting.id] = FakeRecord(meeting)
        await self.pool.add_context(meeting.id, meeting.context)
        await self.pool.cancel(meeting.id)
        self.assertEqual(self.daemon.handoffs[-1].meeting_id, meeting.id)
        self.assertEqual(self.launchers, {})


class PoolComposeTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_pool_meeting_is_its_own_compose_project(self):
        runner = ComposeRunner()
        env = {'SMITLINE_MEETING_ID': 'mtg-first00000001', 'SMITLINE_MEETING_VNC_PORT': '18100'}
        agent = ComposeMeetingAgent(runner, environ={}, project='smitline-meeting-mtg-first00000001', env=env)
        await agent.up({'MEETING_URL': 'x'})
        await agent.inspect()
        await agent.stop()
        compose = ['docker', 'compose', '-p', 'smitline-meeting-mtg-first00000001',
                   '-f', 'compose.meeting.pool.yaml']
        self.assertEqual(runner.calls, [
            [*compose, 'up', '-d', '--build', 'meeting-agent'],
            [*compose, 'ps', '-a', '--format', 'json'],
            # A finished pool meeting's container is removed, not only stopped.
            [*compose, 'down', '--remove-orphans'],
        ])
        image, _ = compose_command({'SMITLINE_MEETING_IMAGE': 'x'}, project='p')
        self.assertEqual(image, ['docker', 'compose', '-p', 'p', '-f',
                                 str(CODE_ROOT / 'compose.meeting.pool.image.yaml')])

    def test_pool_compose_files_mount_no_shared_data(self):
        for name in ('compose.meeting.pool.yaml', 'compose.meeting.pool.image.yaml'):
            if not (CODE_ROOT / name).is_file():
                self.skipTest('the compose files are not in the meeting image')
            text = (CODE_ROOT / name).read_text()
            self.assertNotIn('6082', text)
            self.assertNotIn('8094:8094', text)
            self.assertNotIn('- data:/data', text)
            self.assertNotIn('SMITLINE_AUTH_MODE', text)
            for key in ('SMITLINE_MEETING_VNC_PORT', 'SMITLINE_MEETING_HEALTH_PORT',
                        'SMITLINE_MEETING_RUN_DIR', 'SMITLINE_MEETING_RECORDINGS_DIR',
                        'SMITLINE_MEETING_PROFILES_DIR', 'SMITLINE_MEETING_ENV_FILE'):
                self.assertIn(key, text, (name, key))


class SettingsTests(unittest.TestCase):
    def test_one_meeting_at_a_time_is_the_default(self):
        self.assertEqual(pool_settings({}), {'max_meetings': 1, 'per_owner': None, 'ports': (18100, 18399)})
        self.assertEqual(pool_settings({'SMITLINE_MAX_MEETINGS': '4', 'SMITLINE_MAX_MEETINGS_PER_OWNER': '2',
                                        'SMITLINE_MEETING_PORTS': '20000-20099'}),
                         {'max_meetings': 4, 'per_owner': 2, 'ports': (20000, 20099)})
        for bad in ({'SMITLINE_MAX_MEETINGS': '0'}, {'SMITLINE_MAX_MEETINGS': 'many'},
                    {'SMITLINE_MAX_MEETINGS_PER_OWNER': '-1'}, {'SMITLINE_MEETING_PORTS': '80-90'},
                    {'SMITLINE_MEETING_PORTS': '20000'}, {'SMITLINE_MEETING_PORTS': '20099-20000'}):
            with self.subTest(env=bad), self.assertRaises(ValueError):
                pool_settings(bad)

    def test_the_daemon_picks_the_single_supervisor_or_the_pool(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertIsInstance(build_supervisor(root, root / 'meeting-runtime', {}),
                                  ProductionMeetingSupervisor)
            pool = build_supervisor(root, root / 'meeting-runtime', {'SMITLINE_MAX_MEETINGS': '3'})
            self.assertIsInstance(pool, MeetingPool)
            self.assertEqual(pool.max_meetings, 3)
            # A deployment that serves accounts keeps meetings apart even one at a time.
            hosted = build_supervisor(root, root / 'meeting-runtime', {'SMITLINE_REQUIRE_OWNER': '1'})
            self.assertIsInstance(hosted, MeetingPool)
            self.assertEqual(hosted.max_meetings, 1)


if __name__ == '__main__':
    unittest.main()
