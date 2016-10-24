'''Generic utility jobs.'''

import subprocess

from . import logger


class SubprocessJob(object):
    '''
    A job that runs a command with arguments using subprocess. The job data is as follows:

    {
        "command": "...",
        "args": ["...", "...", ...]
    }
    '''

    @staticmethod
    def process(job):
        '''Process the job.'''
        args = [job.data['command']] + job.data.get('args', [])
        argstring = ' '.join(args)
        with open('/dev/null') as stdin:
            logger.info('Opening subprocess: %s in %s', argstring, job.sandbox)
            proc = subprocess.Popen(
                args,
                stdin=stdin,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=job.sandbox)
            logger.info('Opened subprocess (%i): %s', proc.pid, argstring)
            try:
                if proc.wait() != 0:
                    group = 'subprocess-failed'
                    message = '\n\n'.join([
                        'Exit code: %i' % proc.returncode,
                        'Stderr (last 1k): %s' % proc.stderr.read()[-1000:],
                        'Stdout (last 1k): %s' % proc.stdout.read()[-1000:]
                    ])
                    job.fail(group, message)
                else:
                    job.complete()
            finally:
                try:
                    logger.info('Killing subprocess (%i): %s', proc.pid, argstring)
                    proc.kill()
                except OSError:
                    logger.info('Subprocess already dead.')
                logger.info('Waiting for subprocess (%i): %s', proc.pid, argstring)
                proc.wait()
