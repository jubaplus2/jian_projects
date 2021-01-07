from __future__ import absolute_import, division, print_function, unicode_literals

from ast import literal_eval
import errno
import logging
from operator import itemgetter
import os
from os.path import isdir, isfile, join
import re
import sys
import time
import warnings

from .base.constants import DEFAULTS_CHANNEL_NAME
from .common.compat import ensure_text_type, iteritems, open, text_type
from .core.prefix_data import PrefixData, linked
from .exceptions import CondaFileIOError, CondaHistoryError
from .gateways.disk.update import touch
from .models.dist import Dist
from .resolve import MatchSpec

try:
    from cytoolz.itertoolz import groupby
except ImportError:  # pragma: no cover
    from ._vendor.toolz.itertoolz import groupby  # NOQA


log = logging.getLogger(__name__)


class CondaHistoryWarning(Warning):
    pass


def write_head(fo):
    fo.write("==> %s <==\n" % time.strftime('%Y-%m-%d %H:%M:%S'))
    fo.write("# cmd: %s\n" % (' '.join(ensure_text_type(s) for s in sys.argv)))


def is_diff(content):
    return any(s.startswith(('-', '+')) for s in content)


def pretty_diff(diff):
    added = {}
    removed = {}
    for s in diff:
        fn = s[1:]
        dist = Dist(fn)
        name, version, _, channel = dist.quad
        if channel != DEFAULTS_CHANNEL_NAME:
            version += ' (%s)' % channel
        if s.startswith('-'):
            removed[name.lower()] = version
        elif s.startswith('+'):
            added[name.lower()] = version
    changed = set(added) & set(removed)
    for name in sorted(changed):
        yield ' %s  {%s -> %s}' % (name, removed[name], added[name])
    for name in sorted(set(removed) - changed):
        yield '-%s-%s' % (name, removed[name])
    for name in sorted(set(added) - changed):
        yield '+%s-%s' % (name, added[name])


def pretty_content(content):
    if is_diff(content):
        return pretty_diff(content)
    else:
        return iter(sorted(content))


class History(object):

    def __init__(self, prefix):
        self.prefix = prefix
        self.meta_dir = join(prefix, 'conda-meta')
        self.path = join(self.meta_dir, 'history')

    def __enter__(self):
        self.init_log_file()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.update()

    def init_log_file(self):
        touch(self.path, True)

    def file_is_empty(self):
        return os.stat(self.path).st_size == 0

    def update(self):
        """
        update the history file (creating a new one if necessary)
        """
        try:
            try:
                last = set(self.get_state())
            except CondaHistoryError as e:
                warnings.warn("Error in %s: %s" % (self.path, e),
                              CondaHistoryWarning)
                return
            curr = set(map(str, linked(self.prefix)))
            self.write_changes(last, curr)
        except IOError as e:
            if e.errno == errno.EACCES:
                log.debug("Can't write the history file")
            else:
                raise CondaFileIOError(self.path, "Can't write the history file %s" % e)

    def parse(self):
        """
        parse the history file and return a list of
        tuples(datetime strings, set of distributions/diffs, comments)
        """
        res = []
        if not isfile(self.path):
            return res
        sep_pat = re.compile(r'==>\s*(.+?)\s*<==')
        with open(self.path) as f:
            lines = f.read().splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            m = sep_pat.match(line)
            if m:
                res.append((m.group(1), set(), []))
            elif line.startswith('#'):
                res[-1][2].append(line)
            elif len(res) > 0:
                res[-1][1].add(line)
        return res

    def get_user_requests(self):
        """
        return a list of user requested items.  Each item is a dict with the
        following keys:
        'date': the date and time running the command
        'cmd': a list of argv of the actual command which was run
        'action': install/remove/update
        'specs': the specs being used
        """
        res = []
        com_pat = re.compile(r'#\s*cmd:\s*(.+)')
        spec_pat = re.compile(r'#\s*(\w+)\s*specs:\s*(.+)?')
        for dt, unused_cont, comments in self.parse():
            item = {'date': dt}
            for line in comments:
                m = com_pat.match(line)
                if m:
                    argv = m.group(1).split()
                    if argv[0].endswith('conda'):
                        argv[0] = 'conda'
                    item['cmd'] = argv
                m = spec_pat.match(line)
                if m:
                    action, specs = m.groups()
                    item['action'] = action
                    specs = specs or ""
                    if specs.startswith('['):
                        specs = literal_eval(specs)
                    elif '[' not in specs:
                        specs = specs.split(',')
                    specs = [spec for spec in specs if spec and not spec.endswith('@')]
                    if specs and action in ('update', 'install', 'create'):
                        item['update_specs'] = item['specs'] = specs
                    elif specs and action in ('remove', 'uninstall'):
                        item['remove_specs'] = item['specs'] = specs

            if 'cmd' in item:
                res.append(item)
            dists = groupby(itemgetter(0), unused_cont)
            item['unlink_dists'] = dists.get('-', ())
            item['link_dists'] = dists.get('+', ())
        return res

    def get_requested_specs_map(self):
        # keys are package names and values are specs
        spec_map = {}
        for request in self.get_user_requests():
            remove_specs = (MatchSpec(spec) for spec in request.get('remove_specs', ()))
            for spec in remove_specs:
                spec_map.pop(spec.name, None)
            update_specs = (MatchSpec(spec) for spec in request.get('update_specs', ()))
            spec_map.update(((s.name, s) for s in update_specs))

        # Conda hasn't always been good about recording when specs have been removed from
        # environments.  If the package isn't installed in the current environment, then we
        # shouldn't try to force it here.
        prefix_recs = tuple(PrefixData(self.prefix).iter_records())
        return dict((name, spec) for name, spec in iteritems(spec_map)
                    if any(spec.match(dist) for dist in prefix_recs))

    def construct_states(self):
        """
        return a list of tuples(datetime strings, set of distributions)
        """
        res = []
        cur = set([])
        for dt, cont, unused_com in self.parse():
            if not is_diff(cont):
                cur = cont
            else:
                for s in cont:
                    if s.startswith('-'):
                        cur.discard(s[1:])
                    elif s.startswith('+'):
                        cur.add(s[1:])
                    else:
                        raise CondaHistoryError('Did not expect: %s' % s)
            res.append((dt, cur.copy()))
        return res

    def get_state(self, rev=-1):
        """
        return the state, i.e. the set of distributions, for a given revision,
        defaults to latest (which is the same as the current state when
        the log file is up-to-date)
        """
        states = self.construct_states()
        if not states:
            return set([])
        times, pkgs = zip(*states)
        return pkgs[rev]

    def print_log(self):
        for i, (date, content, unused_com) in enumerate(self.parse()):
            print('%s  (rev %d)' % (date, i))
            for line in pretty_content(content):
                print('    %s' % line)
            print()

    def object_log(self):
        result = []
        for i, (date, content, unused_com) in enumerate(self.parse()):
            # Based on Mateusz's code; provides more details about the
            # history event
            event = {
                'date': date,
                'rev': i,
                'install': [],
                'remove': [],
                'upgrade': [],
                'downgrade': []
            }
            added = {}
            removed = {}
            if is_diff(content):
                for pkg in content:
                    name, version, build, channel = Dist(pkg[1:]).quad
                    if pkg.startswith('+'):
                        added[name.lower()] = (version, build, channel)
                    elif pkg.startswith('-'):
                        removed[name.lower()] = (version, build, channel)

                changed = set(added) & set(removed)
                for name in sorted(changed):
                    old = removed[name]
                    new = added[name]
                    details = {
                        'old': '-'.join((name,) + old),
                        'new': '-'.join((name,) + new)
                    }

                    if new > old:
                        event['upgrade'].append(details)
                    else:
                        event['downgrade'].append(details)

                for name in sorted(set(removed) - changed):
                    event['remove'].append('-'.join((name,) + removed[name]))

                for name in sorted(set(added) - changed):
                    event['install'].append('-'.join((name,) + added[name]))
            else:
                for pkg in sorted(content):
                    event['install'].append(pkg)
            result.append(event)
        return result

    def write_changes(self, last_state, current_state):
        if not isdir(self.meta_dir):
            os.makedirs(self.meta_dir)
        with open(self.path, 'a') as fo:
            write_head(fo)
            for fn in sorted(last_state - current_state):
                fo.write('-%s\n' % fn)
            for fn in sorted(current_state - last_state):
                fo.write('+%s\n' % fn)

    def write_specs(self, remove_specs=(), update_specs=()):
        remove_specs = [text_type(MatchSpec(s)) for s in remove_specs]
        update_specs = [text_type(MatchSpec(s)) for s in update_specs]
        if update_specs or remove_specs:
            with open(self.path, 'a') as fh:
                if remove_specs:
                    fh.write("# remove specs: %s\n" % remove_specs)
                if update_specs:
                    fh.write("# update specs: %s\n" % update_specs)


if __name__ == '__main__':
    from pprint import pprint
    # Don't use in context manager mode---it augments the history every time
    h = History(sys.prefix)
    pprint(h.get_user_requests())
    print(h.get_requested_specs())
