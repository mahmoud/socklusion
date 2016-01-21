# -*- coding: utf-8 -*-
"""Socklusion is a single-file module and command that provides
simple, isolated socket communication. No threads or dependencies
beyond Python 2.6+. Worry-free, cross-platform, isolated, parallel
message sending.

Practically, socklusion (pronounced like "seclusion") is used to
"fire and forget" network messages. One such use case is the sending
of analytics[1], as one might do from the browser. Specifically, the
version/build of Python, PATH settings, and other environment
information.

Technically, socklusion uses the Python [subprocess][2] module to spin
up a miniature, daemonized process that handles sending the message
and receiving the response. The parent process (your application) only
waits on the process spawn time, not the network time, and can
continue or terminate without worrying about zombie processes or
interrupting the transmission.

On a 2013 laptop running Python 2.7.6 on Ubuntu 14 with Linux 3.13,
each socklusion takes around 20 milliseconds to return.

Socklusion's networking is done in a disowned/daemonized process, so
feedback is limited to files. Nothing is saved by default, as
socklusion is designed for fire-and-forget messages.  The response, if
there is one, can be saved to a file, as can debugging information, if
there is an exception. See the command help and docstrings for more.

[1]: https://en.wikipedia.org/wiki/Web_analytics
[2]: https://docs.python.org/2/library/subprocess.html

"""
# TODO: retries?
# TODO: Python 3 probably
# TODO: use close_fds?

import os
import sys
import time
import socket
import optparse
import traceback
import subprocess

from pipes import quote as shell_quote

DEFAULT_PORT = 80  # default to HTTP port
DEFAULT_TIMEOUT = 60.0
DEFAULT_SOCKET_TIMEOUT = 5.0


PYTHON = sys.executable
CUR_FILE = os.path.abspath(__file__)


def parse_args():
    prs = optparse.OptionParser(description="Socklusion provides simple,"
                                " isolated socket communication.")
    ao = prs.add_option

    host_help = 'The target host.'
    if socket.has_ipv6:
        host_help += ' Can be a hostname, FQDN, IPv4, or IPv6 address.'
    else:
        host_help += ' Can be a hostname, FQDN, or IPv4 address.'
    ao('--host', help=host_help)
    ao('--port', type=int, help='The target port. Default %r.' % DEFAULT_PORT)
    ao('--message',
       help=r"The message to send, as a Python string literal."
       r" (e.g., 'POST / HTTP/1.0\nContent-Length: 2\n\n{}\n')"
       r" If missing, message will be read from stdin (useful for piping).")
    ao('--send-only', action='store_true',
       help="Do not wait for a response from the server.")
    ao('--response-file', dest='response_path',
       help="Save the response to a file at this path. Will not be created"
       " if there is an exception, no response, or --send-only is enabled."
       " Will overwrite.")
    ao('--exception-file', dest='exception_path',
       help="Save debug info to this file if there is an exception.")
    ao('--mode', default='parent',
       help="(For internal and testing use.)")
    ao('--timeout', type=float, default=DEFAULT_TIMEOUT,
       help="Total seconds for process lifetime. Default is %r."
       % DEFAULT_TIMEOUT)
    ao('--socket-timeout', type=float, default=DEFAULT_SOCKET_TIMEOUT,
       help="Seconds allowed for each socket operation, including connect."
       " Default is %r." % DEFAULT_SOCKET_TIMEOUT)

    opts, args = prs.parse_args()

    if opts.host is None:
        prs.error('host is required. Use --help for more info.')

    return _get_opt_map(opts), args


def _get_opt_map(values_obj):
    attr_names = set(dir(values_obj)) - set(dir(optparse.Values()))
    return dict([(an, getattr(values_obj, an)) for an in attr_names])


def build_command(host, port=None, timeout=None,
                  socket_timeout=None, send_only=None,
                  response_path=None, exception_path=None, mode=None):
    cmd_tokens = [PYTHON, CUR_FILE]

    if mode:
        cmd_tokens += ['--mode', mode]

    cmd_tokens += ['--host', str(host)]
    if port:
        cmd_tokens += ['--port', str(port)]
    if send_only:
        cmd_tokens += ['--send-only']
    if timeout is not None:
        cmd_tokens += ['--timeout', str(timeout)]
    if socket_timeout is not None:
        cmd_tokens += ['--socket-timeout', str(socket_timeout)]
    if response_path:
        cmd_tokens += ['--response-file', os.path.abspath(response_path)]
    if exception_path:
        cmd_tokens += ['--exception-file', os.path.abspath(exception_path)]

    return cmd_tokens


def get_command_str():
    return ' '.join([sys.executable] + [shell_quote(v) for v in sys.argv])


def send_data(data, **kwargs):
    # TODO: detect if win32. may not need surrogate in that case.

    new_kwargs = dict(kwargs)
    new_kwargs['mode'] = 'surrogate'
    cmd = build_command(**new_kwargs)

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    proc.stdin.write(data)
    proc.stdin.flush()
    proc.stdin.close()

    return proc.wait()


def send_data_surrogate(data, **kwargs):
    new_kwargs = dict(kwargs)
    new_kwargs['mode'] = 'child'
    cmd = build_command(**new_kwargs)

    os.chdir('/')
    try:
        os.setsid()
    except AttributeError:
        pass  # win32 has no terminal sessions
    os.umask(0)

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    proc.stdin.write(data)
    proc.stdin.flush()
    proc.stdin.close()

    return 0


def send_data_child(data, **kwargs):
    exception_path = kwargs.pop('exception_path', None)
    if not exception_path:
        return _send_data_inner(data, **kwargs)

    try:
        ret = _send_data_inner(data, **kwargs)
    except Exception:
        try:
            with open(exception_path, 'wb') as f:
                f.write('traceback = """\\\n')
                f.write(traceback.format_exc())
                f.write('"""\n')
                f.write('command = ')
                f.write(repr(get_command_str()))
                f.write('\n\n')
                f.write('message = ')
                f.write(repr(data or ''))
                f.write('\n')
        except Exception:
            pass
        raise
    return ret


def _daemonize_streams():
    stdin = open(os.devnull, 'r')
    os.dup2(stdin.fileno(), sys.stdin.fileno())

    stdout = open(os.devnull, 'a+')
    os.dup2(stdout.fileno(), sys.stdout.fileno())

    stderr = open(os.devnull, 'a+')
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    return


def _send_data_inner(data, host, port=None, send_only=None,
                     timeout=None, socket_timeout=None, response_path=None,
                     mode=None):

    timeout = DEFAULT_TIMEOUT if timeout is None else float(timeout)
    if socket_timeout:
        socket_timeout = float(socket_timeout)
    else:
        socket_timeout = DEFAULT_SOCKET_TIMEOUT
    socket_timeout = min(socket_timeout, timeout)

    if not host:
        raise ValueError('expected host, not %r' % host)
    if not port:
        port = DEFAULT_PORT

    start_time = time.time()
    max_time = start_time + timeout
    sock = socket.create_connection((host, port), timeout=socket_timeout)

    sock.sendall(data)

    if send_only:
        sock.shutdown(socket.SHUT_WR)
        sock.recv(1)  # wait for the empty read
        sock.close()
        return

    response_file = None
    while 1:
        cur_time = time.time()
        if cur_time > max_time:
            raise RuntimeError('timed out after %r seconds'
                               % (cur_time - start_time))
        data = sock.recv(4096)
        if not data:
            break
        if response_path and not response_file:
            response_file = open(response_path, 'wb')

        if response_file:
            response_file.write(data)
            response_file.flush()

    sock.shutdown(socket.SHUT_WR)
    sock.recv(1)  # wait for the empty read
    sock.close()

    if response_file:
        response_file.close()
    return


def main():
    opts, args = parse_args()
    kwargs = dict(opts)

    message = kwargs.pop('message', None)
    if message is not None:
        message = message.decode('string_escape')
    else:
        message = sys.stdin.read()

    mode = kwargs.pop('mode')
    if mode == 'parent':
        # start_time = time.time()
        ret = send_data(message, **kwargs)
        # print round((time.time() - start_time) * 1000, 2), 'ms'
    elif mode == 'surrogate':
        ret = send_data_surrogate(message, **kwargs)
    elif mode == 'child':
        _daemonize_streams()
        ret = send_data_child(message, **kwargs)
    else:
        raise ValueError('invalid mode %r' % mode)

    sys.exit(ret)


if __name__ == '__main__':
    main()
