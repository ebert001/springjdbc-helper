javadoc乱码，解决方案：
添加环境变量：
变量名: JAVA_TOOL_OPTIONS
 变量值: -Dfile.encoding=UTF-8
配置完成后，需要重启系统
 验证方式：mvn -version
输出内容中，字符集需要变成 UTF-8

GPG签名
下载Gpg4win，并安装。安装完成后生成密钥。重启操作系统。打开eclipse，并运行maven install，提示输入gpg密钥，则说明安装成功.
