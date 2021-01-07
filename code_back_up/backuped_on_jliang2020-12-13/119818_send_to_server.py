import paramiko
import shutil
paramiko.util.log_to_file('/tmp/paramiko.log')
import os

class MySFTPClient(paramiko.SFTPClient):
    def put_dir(self, source, target):
        ''' Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are 
            created under target.
        '''
        for item in os.listdir(source):
            if os.path.isfile(os.path.join(source, item)):
                self.put(os.path.join(source, item), '%s/%s' % (target, item))
            else:
                self.mkdir('%s/%s' % (target, item), ignore_existing=True)
                self.put_dir(os.path.join(source, item), '%s/%s' % (target, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        ''' Augments mkdir by adding an option to not fail if the folder exists  '''
        try:
            super(MySFTPClient, self).mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise

class SSHConnection(object):
    """"""
 
    #----------------------------------------------------------------------
    def __init__(self, host, username, password, port=22):
        """Initialize and setup connection"""
        self.sftp = None
        self.sftp_open = False
 
        # open SSH Transport stream
        self.transport = paramiko.Transport((host, port))
 
        self.transport.connect(username=username, password=password)
 
    #----------------------------------------------------------------------
    def _openSFTPConnection(self):
        """
        Opens an SFTP connection if not already open
        """
        if not self.sftp_open:
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            self.sftp_open = True
 
    #----------------------------------------------------------------------
    def get(self, remote_path, local_path=None):
        """
        Copies a file from the remote host to the local host.
        """
        self._openSFTPConnection()        
        self.sftp.get(remote_path, local_path)        
 
    #----------------------------------------------------------------------
    def put(self, local_path, remote_path=None):
        """
        Copies a file from the local host to the remote host
        """
        self._openSFTPConnection()
        self.sftp.put(local_path, remote_path)
 
    #----------------------------------------------------------------------
    def close(self):
        """
        Close SFTP connection and ssh connection
        """
        if self.sftp_open:
            self.sftp.close()
            self.sftp_open = False
        self.transport.close()

if __name__ == "__main__":
    host = "64.237.51.251"
    username = "ruoyi"
    pw = "mushiwy009"

    origin_dir = '/Users/Mushi/Desktop/test_dir/'
    dst_dir = '/home/ruoyi/test_dir/'
    
    ssh = SSHConnection(host, username, pw)
    ssh.put(origin, dst)
    ssh.close()

    #-----directory-------------------------------------------------------
    transport = paramiko.Transport((host, 22))
    transport.connect(username=username, password=pw)
    sftp = MySFTPClient.from_transport(transport)
    sftp.mkdir(dst_dir, ignore_existing=True)
    sftp.put_dir(origin_dir, dst_dir)
    sftp.close()

