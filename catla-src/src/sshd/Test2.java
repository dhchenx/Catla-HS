package cn.edu.bjtu.cdh.catla.sshd;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.future.AuthFuture;
import org.apache.sshd.client.future.ConnectFuture;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.SftpClient.DirEntry;
import org.apache.sshd.client.subsystem.sftp.SftpClientFactory;

public class Test2 {

	public static void main(String[] args) {
		try {
			// TODO Auto-generated method stub
			String LOCAL_FILE="uploads/cacjoin.jar";
			String REMOTE_FILE="cacjoin.jar";
			String DOWNLOAD_FILE="downloads/cacjoin.jar";

			SshClient client = SshClient.setUpDefaultClient();
			client.start();

			ConnectFuture connect = client.connect("hadoop", "192.168.159.132", 22);
			boolean isSuccess = connect.await(5000L);

			if (isSuccess) {
				ClientSession session = connect.getSession();
				session.addPasswordIdentity("Passw0rd");

				// session.addPublicKeyIdentity(SecurityUtils.loadKeyPairIdentity("keyname", new
				// FileInputStream("priKey.pem"), null));

				AuthFuture auth = session.auth();
				auth.await();
				if (!auth.isSuccess())
					System.out.println("auth failed");
				SftpClient sftp = SftpClientFactory.instance().createSftpClient(session);

				for (DirEntry de : sftp.readDir("."))
					System.out.println(de.getFilename() + " " + de.getAttributes().getType());

				Path src = Paths.get(LOCAL_FILE);
				sftp.remove(REMOTE_FILE);

				OutputStream os = sftp.write(REMOTE_FILE);
				Files.copy(src, os);
				os.close();
				
				InputStream is = sftp.read(REMOTE_FILE);
				Path dst = Paths.get(DOWNLOAD_FILE);
				Files.deleteIfExists(dst);
				Files.copy(is, dst);
				is.close();

				sftp.close();
				client.stop();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
