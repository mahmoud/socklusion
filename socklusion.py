# -*- coding: utf-8 -*-

import os
import ssl
import sys
import time
import socket
import optparse
import traceback
import subprocess

from pipes import quote as shell_quote


DEFAULT_TIMEOUT = 60.0
DEFAULT_SOCKET_TIMEOUT = 1.0

PYTHON = sys.executable
CUR_FILE = os.path.abspath(__file__)


def parse_args():
    # TODO: help strings
    prs = optparse.OptionParser()
    ao = prs.add_option
    ao('--host')
    ao('--port', type=int)
    ao('--wrap-ssl', action='store_true')
    ao('--message')
    ao('--send-only', action='store_true')
    ao('--response-path')
    ao('--exception-path')
    ao('--timeout', type=float, default=DEFAULT_TIMEOUT)
    ao('--socket-timeout', type=float, default=DEFAULT_SOCKET_TIMEOUT)
    ao('--mode', default='parent')

    opts, args = prs.parse_args()

    return _get_opt_map(opts), args


def _get_opt_map(values_obj):
    attr_names = set(dir(values_obj)) - set(dir(optparse.Values()))
    return dict([(an, getattr(values_obj, an)) for an in attr_names])


def build_command(host, port=None, wrap_ssl=None, timeout=None,
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
        cmd_tokens += ['--response-path', os.path.abspath(response_path)]
    if exception_path:
        cmd_tokens += ['--exception-path', os.path.abspath(exception_path)]
    if wrap_ssl:
        cmd_tokens += ['--wrap-ssl']

    return cmd_tokens


def get_command_str():
    return ' '.join([sys.executable] + [shell_quote(v) for v in sys.argv])


def send_data_parent(data, **kwargs):
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


def send_data(data, **kwargs):
    exception_path = kwargs.pop('exception_path', None)
    if not exception_path:
        return _send_data_inner(data, **kwargs)

    try:
        ret = _send_data_inner(data, **kwargs)
    except Exception:
        # TODO get all this stuff escaped correctly
        try:
            with open(exception_path, 'wb') as f:
                f.write('traceback = """\\\n')
                f.write(traceback.format_exc())
                f.write('"""\n')
                f.write('command = ')
                f.write(repr(get_command_str()))
                f.write('\n\n')
                f.write('message = """\\\n')
                f.write(data or '')
                f.write('"""\n')

        except:
            pass
        raise
    return ret


def tquote_repr(instr):
    # lines = [r'"""\']
    for line in instr.splitlines():
        pass


def _daemonize_streams():
    stdin = open(os.devnull, 'r')
    os.dup2(stdin.fileno(), sys.stdin.fileno())

    stdout = open(os.devnull, 'a+')
    os.dup2(stdout.fileno(), sys.stdout.fileno())

    stderr = open(os.devnull, 'a+', 0)
    os.dup2(stderr.fileno(), sys.stderr.fileno())

    return


def _send_data_inner(data, host, port=None, wrap_ssl=None, send_only=None,
                     timeout=None, socket_timeout=None, response_path=None,
                     mode=None):

    timeout = DEFAULT_TIMEOUT if timeout is None else float(timeout)
    if not socket_timeout:
        socket_timeout = DEFAULT_SOCKET_TIMEOUT
    else:
        socket_timeout = float(socket_timeout)
    socket_timeout = min(socket_timeout, timeout)

    if not host:
        raise ValueError('expected host, not %r' % host)
    if not port:
        # default to HTTP(S) ports
        if wrap_ssl:
            port = 443
        else:
            port = 80

    start_time = time.time()
    max_time = start_time + timeout
    sock = socket.socket()
    if wrap_ssl:
        sock = ssl.wrap_socket(sock)  # TODO: test
    sock.settimeout(socket_timeout)
    sock.connect((host, port))
    sock.sendall(data)
    if send_only:
        sock.shutdown(socket.SHUT_WR)
        sock.recv(1)  # wait for the empty read
        return

    response_file = None
    while 1:
        cur_time = time.time()
        if time.time() > max_time:
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

    if response_file:
        response_file.close()
    return


def get_input_message():
    return sys.stdin.read()


def main():
    opts, args = parse_args()
    kwargs = dict(opts)

    message = kwargs.pop('message', '') or get_input_message()
    mode = kwargs['mode']
    if mode == 'parent':
        # start_time = time.time()
        ret = send_data_parent(message, **kwargs)
        # print round((time.time() - start_time) * 1000, 2), 'ms'
    elif mode == 'surrogate':
        ret = send_data_surrogate(message, **kwargs)
    else:
        _daemonize_streams()
        ret = send_data(message, **kwargs)

    sys.exit(ret)


if __name__ == '__main__':
    main()
