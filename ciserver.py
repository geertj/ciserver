#!/usr/bin/env python
#
# A proof of concept CI server for Ravello/Github.
#

from gevent import monkey; monkey.patch_all()

import os
import os.path
import re
import sys
import json
import logging
import urlparse
import httplib
from httplib import HTTPConnection, HTTPSConnection
import textwrap
import collections
import traceback
from optparse import OptionParser

from gevent import Greenlet, socket, ssl
from gevent.local import local
from gevent.pywsgi import WSGIServer
from gevent.event import Event
import winpexpect
from winpexpect import TIMEOUT, EOF

from ravello.api.client import RavelloClient

winpexpect.default_nbio_class = winpexpect.GEventNBIO

# Constrants - should be no need to change these
ravello_api_url = 'http://cloud.ravellosystems.com/'
github_api_url = 'https://api.github.com/'


def create(cls, *args):
    """Factory method."""
    if issubclass(cls, Scheduler):
        opts = args[0]
        cls.set_api_parameters(opts.api_url, opts.api_user, opts.api_password)
        obj = cls(opts.directory)
        cls.instance = obj
    elif issubclass(cls, WebhookServer):
        opts = args[0]
        obj = cls(opts.webhook_addr)
        cls.instance = obj
    else:
        obj = cls(*args)
        cls.instance = obj
    return obj

def instance(cls):
    """Return the singleton instance for class `cls`."""
    return cls.instance


class Object(dict):
    """A JSON serializable object."""

    @classmethod
    def fromjson(cls, s):
        o = json.loads(s)
        if not isinstance(o, dict):
            raise TypeError('Expecting dictionary.')
        return cls(o)

    @classmethod
    def load(cls, fname):
        with file(fname) as fin:
            return cls.fromjson(fin.read())

    def tojson(self):
        return json.dumps(self, indent=2)

    def save(self, fname):
        with file(fname, 'w') as fout:
            fout.write(self.tojson())


class TestRequest(Object):
    """Test request (via webhook)."""

class TestJob(Object):
    """Test job."""

class Project(Object):
    """Project metadata."""


class GithubWebhook(object):
    """WSGI application that responds to the github post-receive
    hook, parses the request, and puts a TestJob in the TestQueue.
    """

    re_path = re.compile('^/test/([a-zA-Z0-9_-]+)$')

    def __init__(self):
        self.local = local()
        self.logger = logging.getLogger('ciserver.GithubWebhook')

    def __call__(self, env, start_response):
        logger = self.logger
        self.local.environ = env
        self.local.start_response = start_response
        logger.debug('Request: %s %s', env['REQUEST_METHOD'], env['PATH_INFO'])
        method = env['REQUEST_METHOD']
        if method != 'POST':
            return self.simple_response(httplib.METHOD_NOT_ALLOWED)
        path = env['PATH_INFO']
        match = self.re_path.match(path)
        if not match:
            return self.simple_response(httplib.NOT_FOUND)
        project = match.group(1)
        ctype = env.get('CONTENT_TYPE')
        if ctype != 'application/x-www-form-urlencoded':
            return self.simple_response(httplib.UNSUPPORTED_MEDIA_TYPE)
        try:
            body = env['wsgi.input'].read()
            payload = urlparse.parse_qs(body)['payload'][0]
            request = TestRequest.fromjson(payload)
        except (TypeError, ValueError, KeyError, IndexError):
            return self.simple_response(httplib.BAD_REQUEST)
        scheduler = instance(Scheduler)
        if not scheduler.schedule_run(project, request):
            return self.simple_response(httplib.NOT_FOUND)
        return self.simple_response(httplib.ACCEPTED)

    def simple_response(self, code):
        reason = httplib.responses[code]
        status = '%s %s' % (code, reason)
        headers = [('Content-Type', 'text/plain')]
        self.local.start_response(status, headers)
        self.logger.debug('Response: %s', status)
        return [reason]


def parse_address(address):
    """Parse an IP address into a Python address tuple."""
    try:
        host, port = address.split(':')
        if host == '':
            host = '0.0.0.0'
        port = int(port)
    except ValueError:
        raise ValueError('Illegal address: %s' % address)
    return (host, port)


class WebhookServer(WSGIServer):
    """Webhook server. Runs the GithubWebhook application."""

    def __init__(self, listen):
        addr = parse_address(listen)
        super(WebhookServer, self).__init__(addr, GithubWebhook(),
                                            log=None, spawn=10)


class Scheduler(Greenlet):
    """Job scheduler."""

    # Keep max_instances_per_blueprint to 1 until the "do not really
    # provision blueprint" hack/optimization is removed.
    max_vms_per_project = 10
    max_total_vms = 100
    max_instances_per_blueprint = 1

    def __init__(self, directory):
        super(Scheduler, self).__init__()
        self.directory = directory
        self.next_id = 1000
        self.event_queue = []
        self.have_events = Event()
        self.projects = {}
        self.job_queue = collections.deque()
        self.running = {}
        self.project_usage = {}
        self.blueprint_usage = {}
        self.total_vms = 0
        self.logger = logging.getLogger('ciserver.Scheduler')
        self.client = RavelloClient(self.api_url, self.api_user, self.api_password)
        self.load_projects()
        self.load_job_queue()

    @classmethod
    def set_api_parameters(cls, url, user, password):
        cls.api_url = url
        cls.api_user = user
        cls.api_password = password

    def load_projects(self):
        dirname = os.path.join(self.directory, 'projects')
        for fname in os.listdir(dirname):
            if not fname.endswith('.js'):
                continue
            absname = os.path.join(dirname, fname)
            try:
                project = Project.load(absname)
            except (IOError, TypeError, ValueError):
                self.logger.debug('Could not load project: %s', fname)
                continue
            self.projects[project['name']] = project
        self.logger.debug('Loaded %s projects', len(self.projects))

    def load_job_queue(self):
        dirname = os.path.join(self.directory, 'jobs')
        for fname in sorted(os.listdir(dirname)):
            if not fname.endswith('.js'):
                continue
            absname = os.path.join(dirname, fname)
            try:
                job = TestJob.load(absname)
            except (IOError, TypeError, ValueError):
                self.logger.debug('Could not load job: %s', fname)
                continue
            if job['status'] == 'NEW':
                self.job_queue.append(job)
            self.next_id = max(self.next_id, job['id']+1)
        self.logger.debug('Loaded %s jobs', len(self.job_queue))

    def schedule_run(self, project, request):
        if project not in self.projects:
            return False
        project = self.projects[project]
        for env in project['environments']:
            for commit in request['commits']:
                job = TestJob()
                job['project'] = project
                job['request'] = request
                job['environment'] = env
                job['commit'] = commit
                self.add_job(job)
        return True

    def add_job(self, job):
        self.event_queue.append(('AddJob', (job,)))
        self.have_events.set()

    def job_done(self, job, result, message, detail=''):
        self.event_queue.append(('JobDone', (job, result, message, detail)))
        self.have_events.set()

    def _job_filename(self, job):
        return os.path.join(self.directory, 'jobs', '%010d.js' % job['id'])

    def _get_blueprint_allocation(self, name):
        """Return the VMs for a blueprint."""
        apps = self.client.get_applications_metadata()
        for app in apps:
            if app.blueprintName == name:
                return app.numStartedVms
        else:
            return  -1

    def _process_events(self):
        logger = self.logger
        while self.event_queue:
            event, args = self.event_queue.pop(0)
            logger.debug('Handling event %s', event)
            if event == 'AddJob':
                job = args[0]
                job['id'] = self.next_id; self.next_id += 1
                vms = self._get_blueprint_allocation(job['environment']['blueprint'])
                if vms == -1:
                    logger.error('Could not add job %s', job['id'])
                    continue
                job['vms'] = vms
                job['status'] = 'NEW'
                fname = self._job_filename(job)
                try:
                    job.save(fname)
                except IOError:
                    logger.error('Could not add job %s', job['id'])
                    continue
                self.job_queue.append(job)
            elif event == 'JobDone':
                job, result, message, detail = args
                job['status'] = 'DONE'
                job['result'] = { 'result': result, 'message': message, 'detail': detail }
                fname = self._job_filename(job)
                try:
                    job.save(fname)
                except IOError:
                    logger.error('Could not update job %s', job['id'])
                assert job['id'] in self.running
                del self.running[job['id']]
                self.project_usage[job['project']['name']] -= job['vms']
                self.blueprint_usage[job['environment']['blueprint']] -= 1
        logger.debug('Done processing events')

    def _run_jobs(self):
        """A very simple scheduler that can enforce per project caps
        on VMs and blueprints, and a cap on total VMs.
        """
        logger = self.logger
        while True:
            if not self.job_queue:
                logger.debug('Job queue empty')
                break
            logger.debug('Trying to schedule job from job queue')
            # Find the first job whose project is below its cap and
            # whose blueprint is runnable.
            for ix,job in enumerate(self.job_queue):
                project = job['project']['name']
                if self.project_usage.get(project, 0) + job['vms'] \
                            > self.max_vms_per_project:
                    continue
                blueprint = job['environment']['blueprint']
                if self.blueprint_usage.get(blueprint, 0) + 1 \
                            > self.max_instances_per_blueprint:
                    continue
                break
            # If we are running againt the global cap just wait. Don't be
            # smart and try to run other vms as that could starve the current
            # candidate.
            logger.debug('Job candidate: %s', job['id'])
            if self.total_vms + job['vms'] > self.max_total_vms:
                logger.debug('Would run over global cap, not any jobs')
                break
            logger.debug('Still below global cap, running job %s', job['id'])
            # Update allocations
            if project not in self.project_usage:
                self.project_usage[project] = 0
            self.project_usage[project] += job['vms']
            if blueprint not in self.blueprint_usage:
                self.blueprint_usage[blueprint] = 0
            self.blueprint_usage[blueprint] += 1
            self.total_vms += job['vms']
            del self.job_queue[ix]
            self.running[job['id']] = job
            # And finally run the job
            runner = JobRunner(self, job)
            runner.start()
            logger.debug('Created JobRunner to run job')

    def _run(self):
        self.have_events.set()
        self.logger.debug('Entering _run() loop')
        while True:
            self.have_events.wait()
            self.have_events.clear()
            self._process_events()
            self._run_jobs()
        self.logger.debug('_run() loop exited')


class JobFailed(Exception):
    """A job failed."""

    def __init__(self, message, detail=''):
        super(JobFailed, self).__init__(message, detail)


class JobRunner(Greenlet):

    def __init__(self, scheduler, job):
        super(JobRunner, self).__init__()
        self.scheduler = scheduler
        self.job = job
        self.client = RavelloClient(self.api_url, self.api_user, self.api_password)
        self.logger = logging.getLogger('ciserver.JobRunner')
        # Of course this could be made extensible to support multiple
        # controller types, repositories and result types.
        ctrl = self.job['environment']['controller']
        if ctrl['type'] != 'ssh':
            raise ValueError('Unkown controller type: %s' % ctrl['type'])
        repo = self.job['project']['repo']
        if repo['type'] != 'git':
            raise ValueError('Unknown repository type: %s' % repo['type'])
        results = self.job['project']['results']
        if results['type'] != 'github':
            raise ValueError('Unknown repository type: %s' % results['type'])

    @classmethod
    def set_api_parameters(cls, url, user, password):
        cls.api_url = url
        cls.api_user = user
        cls.api_password = password

    def provision_blueprint(self):
        """Provision a Blueprint. Return the application instance ID."""
        # We don't really deploy the new Blueprint. As a POC hack we look for
        # a running app instance that was created from the Blueprint that we're
        # interested in.
        name = self.job['environment']['blueprint']
        apps = self.client.get_applications_metadata()
        for app in apps:
            if app.blueprintName == name:
                break
        else:
            raise JobFailed('Blueprint not found: %s' % name)
        self.appid = app.id

    def run_tests(self):
        """Run the tests via the controller."""
        jobid = self.job['id']
        logger = self.logger
        ctrl = self.job['environment']['controller']
        vms = self.client.meta_vms(self.appid).vms
        for vm in vms:
            if vm.name.lower() == ctrl['host'].lower():
                break
        else:
            raise JobFailed('Controller VM not found: %s' % name)
        host = vm.vmDynamicMD.fullyQualifiedDomainName
        repo = self.job['project']['repo']
        logger.debug('[job %s] Running tests via "ssh" controller', jobid)
        logger.debug('[job %s] Controller node = %s', jobid, host)
        ssh = winpexpect.spawn('ssh-agent sh', timeout=30)
        try:
            # Forward the repository key.
            keyfile = os.path.join(self.scheduler.directory, 'keys', repo['key'])
            ssh.expect('[$#]')
            ssh.send('ssh-add %s\n' % keyfile)
            ssh.expect('[$#]')
            ssh.send('ssh -A ravello@%s\n' % host)
            # Install a more distinctive prompt that hopefully does not occur
            # in the output of any command we run.
            ssh.expect('[#$]')
            prompt = 'CITestHost: '
            ssh.send('PS1="%s"\n' % prompt)
            ssh.expect(prompt)  # echo
            ssh.expect(prompt)  # prompt
            # Check out the commit in a temporary directory
            url = repo['url']
            parsed = urlparse.urlsplit(url)
            #repodir = parsed.path.rstrip('/').split('/')[-1].rstrip('.git')
            logger.debug('[job %s] cloning source code from %s', jobid, url)
            dirname = os.urandom(8).encode('hex')
            ssh.send('mkdir %s\n' % dirname)
            ssh.expect(prompt)
            ssh.send('cd %s\n' % dirname)
            ssh.expect(prompt)
            ssh.send('git clone %s\n' % url)
            ssh.expect(prompt)
            ssh.send('cd *\n')
            ssh.expect(prompt)
            commit = self.job['commit']['id']
            logger.debug('[job %s] checkout commit %s', jobid, commit)
            ssh.send('git checkout %s\n' % commit)
            ssh.expect(prompt)
            # And run the tests!
            command = ctrl['command']
            logger.debug('[job %s] running test command "%s"', jobid, command)
            ssh.send('%s\n' % command)
            ssh.settimeout(600)
            ssh.expect('\r\n')  # echo
            ssh.expect(prompt)
            output = ssh.before[:ssh.before.rfind('\r\n')]  # strip prompt
            ssh.settimeout(30)
            ssh.send('echo $?\n')
            ssh.expect('\r\n')  # echo
            ssh.expect('\r\n')  # end of output
            status = int(ssh.before)
            ssh.expect(prompt)  # end of output
            ssh.send('cd ../..\n')
            ssh.expect(prompt)
            ssh.send('rm -rf %s\n' % dirname)
            ssh.expect(prompt)
            ssh.send(ssh.cchar('VEOF'))  # exit ssh
            ssh.expect('[$#]')
            ssh.send(ssh.cchar('VEOF'))  # exit ssh-agent
            ssh.wait(10)
        except (TIMEOUT, EOF):
            logger.debug('[job %s] failed to run the test', jobid)
            ssh.terminate()
            raise JobFailed('Failed to run test job')
        logger.debug('[job %s] test return code: %s', jobid, status)
        self.status = status
        self.output = output

    commit_ok = 'This commit passed all tests in environment "%(environment)s".'
    issue_title = 'CI Test Error for commit %(commit)s'
    issue_body = textwrap.dedent("""\
        Test suite error for commit %(commit)s for environment "%(environment)s".

        The output of the test suite is:

        %(output)s
        """)

    def publish_results(self):
        """Store the output of the test job."""
        env = self.job['environment']
        commit = self.job['commit']
        results = self.job['project']['results']
        subst = { 'commit': commit['id'], 'environment': env['blueprint'] }
        subst['output'] = '    ' + self.output.replace('\r\n', '\r\n    ')
        client = GithubClient(results['username'], results['repository'],
                              results['token'])
        client.connect()
        if self.status == 0:
            message = self.commit_ok % subst
            client.add_comment_to_commit(commit['id'], message)
        else:
            title = self.issue_title % subst
            body = self.issue_body % subst
            client.add_issue(title, body)
        client.close()
 
    def _run(self):
        logger = self.logger
        jobid = self.job['id']
        logger.debug('[job %s] Running tests for project %s', jobid,
                     self.job['project']['name'])
        try:
            self.provision_blueprint()
            self.run_tests()
            self.publish_results()
        except JobFailed as e:
            self.logger.debug('[job %s] Failed with: %s', jobid, e[0])
            self.scheduler.job_done(self.job, 'FAILED', e[0], e[1])
        except Exception as e:
            lines = ['An uncaught exception occurred\n']
            lines += traceback.format_exception(*sys.exc_info())
            detail = ''.join(lines)
            self.logger.debug('[job %s] Uncaught exception', jobid)
            self.logger.debug(detail)
            self.scheduler.job_done(self.job, 'FAILED', 'Uncaught exception', detail)
        else:
            self.logger.debug('[job %s] Completed successfully', jobid)
            self.scheduler.job_done(self.job, 'OK', 'Job completed successfully')


class GithubClient(object):
    """Simple API client for Github."""

    url = 'https://api.github.com/'

    def __init__(self, user, repo, token):
        self.user = user
        self.repo = repo
        self.token = token
        self.connection = None
        self.logger = logging.getLogger('ciserver.GithubClient')
    
    def connect(self):
        """Connect to the Github API."""
        parsed = urlparse.urlsplit(self.url)
        host = parsed.netloc
        port = parsed.port if parsed.port else httplib.HTTPS_PORT \
                if parsed.scheme == 'https' else httplib.HTTP_PORT
        if parsed.scheme == 'https':
            connection = HTTPSConnection(host, port)
        else:
            connection = HTTPConnection(host, port)
        connection.connect()
        self.logger.debug('Connected to Github at %s:%d' %
                          (connection.host, connection.port))
        self.connection = connection

    def close(self):
        """Close the API connection."""
        self.connection.close()
        self.connection = None

    def make_request(self, method, url, body=None, headers=None):
        if headers is None:
            headers = {}
        headers['Authorization'] = 'token %s' % self.token
        if body is not None:
            body = json.dumps(body, indent=2)
            headers['Content-Type'] = 'application/json'
        self.logger.debug('Github API: %s %s', method, url)
        self.connection.request(method, url, body, headers)
        response = self.connection.getresponse()
        body = response.read()
        ctype = response.getheader('Content-Type', '')
        self.logger.debug('Github API status: %s (%s)', response.status, ctype)
        if ctype.startswith('application/json'):
            response.entity = json.loads(body)
        else:
            response.entity = None
        return response

    def add_issue(self, title, body):
        url = '/repos/%s/%s/issues' % (self.user, self.repo)
        issue = { 'title': title, 'body': body }
        response = self.make_request('POST', url, issue)
        entity = response.entity
        self.logger.debug('Created issue %s', entity['url'])

    def add_comment_to_commit(self, commit, body):
        url = '/repos/%s/%s/commits/%s/comments' % (self.user, self.repo, commit)
        comment = { 'commit_id': commit, 'body': body }
        response = self.make_request('POST', url, comment)
        entity = response.entity
        self.logger.debug('Created commit comment %s', entity['url'])


def test_api_parameters(url, user, password):
    """Try to connect to the Ravello API with the supplied parameters."""
    try:
        client = RavelloClient(url, user, password)
    except Exception as e:
        return False
    return True

def setup_logging(debug):
    """Configure the logging subsystem."""
    logger = logging.getLogger('ciserver')
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = '%(asctime)s %(levelname)s %(name)s %(message)s'
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)


def main(argv):
    parser = OptionParser(description='Continous Integration Server')
    parser.add_option('-u', '--api-url', help='specify the API URL')
    parser.add_option('-n', '--api-user', help='API username')
    parser.add_option('-p', '--api-password', help='API password')
    parser.add_option('-r', '--directory', help='data directory')
    parser.add_option('-w', '--webhook-addr', help='webhook listen address')
    parser.add_option('-d', '--debug', action='store_true',
                      help='show debugging output')
    parser.set_default('api_url', 'http://cloud.ravellosystems.com/')
    parser.set_default('directory', '/etc/ciserver')
    parser.set_default('webhook_addr', ':80')
    opts, args = parser.parse_args(argv)
    if not opts.api_user:
        parser.error('you must supply --api-user\n')
    if not opts.api_password:
        parser.error('you must supply --api-password\n')
    setup_logging(opts.debug)
    if not test_api_parameters(opts.api_url, opts.api_user, opts.api_password):
        sys.stderr.write('Error: could not connect to API at %s\n' % opts.api_url)
        sys.exit(1)
    JobRunner.set_api_parameters(opts.api_url, opts.api_user, opts.api_password)
    scheduler = create(Scheduler, opts)
    scheduler.start()
    server = create(WebhookServer, opts)
    server.start()
    logger = logging.getLogger('ciserver')
    logger.info('Ravello Continous Integration Server started')
    logger.info('Using API at %s' % opts.api_url)
    logger.info('Github webhook listening on %s:%d' % server.address)
    logger.info('Press CTRL-C to end')
    scheduler.join()

if __name__ == '__main__':
    main(sys.argv)
