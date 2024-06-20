package util

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/pkg/errors"
	"k8s.io/klog/v2"

	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// StringContains check if contains string in array
func StringContains(arr []string, str string) bool {
	for _, s := range arr {
		if s == str {
			return true
		}
	}
	return false
}

// GetSourceName returns the field name in message for the given ClickHouse column
func GetSourceName(name string) (sourcename string) {
	sourcename = strings.Replace(name, ".", "\\.", -1)
	return
}

// GetShift returns the smallest `shift` which 1<<shift is no smaller than s
func GetShift(s int) (shift int) {
	for shift = 0; (1 << shift) < s; shift++ {
	}
	return
}

// GetOutboundIP get preferred outbound ip of this machine
// https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		klog.Error(err)
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}

func GetLocalIPv4() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		klog.Error(err)
		return nil, err
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.To4(), nil
			}
		}
	}
	return nil, fmt.Errorf("no found ip")
}

// GetSpareTCPPort find a spare TCP port
func GetSpareTCPPort(ip string, portBegin int) (port int) {
LOOP:
	for port = portBegin; ; port++ {
		addr := fmt.Sprintf("%s:%d", ip, port)
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			ln.Close()
			break LOOP
		}
	}
	return
}

// NewTLSConfig Refers to:
// https://medium.com/processone/using-tls-authentication-for-your-go-kafka-client-3c5841f2a625
// https://github.com/denji/golang-tls
// https://www.baeldung.com/java-keystore-truststore-difference
func NewTLSConfig(caCertFiles, clientCertFile, clientKeyFile string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := tls.Config{}
	// Load client cert
	if clientCertFile != "" && clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			err = errors.Wrapf(err, "")
			return &tlsConfig, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA cert
	caCertPool := x509.NewCertPool()
	for _, caCertFile := range strings.Split(caCertFiles, ",") {
		caCert, err := ioutil.ReadFile(caCertFile)
		if err != nil {
			err = errors.Wrapf(err, "")
			return &tlsConfig, err
		}
		caCertPool.AppendCertsFromPEM(caCert)
	}
	tlsConfig.RootCAs = caCertPool
	tlsConfig.InsecureSkipVerify = insecureSkipVerify
	return &tlsConfig, nil
}

func EnvStringVar(value *string, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		*value = val
	}
}

func EnvIntVar(value *int, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	val, found := os.LookupEnv(realKey)
	if found {
		valInt, err := strconv.Atoi(val)
		if err == nil {
			*value = valInt
		}
	}
}

func EnvBoolVar(value *bool, key string) {
	realKey := strings.ReplaceAll(strings.ToUpper(key), "-", "_")
	_, found := os.LookupEnv(realKey)
	if found {
		*value = true
	}
}

// JksToPem converts JKS to PEM
// Refers to:
// https://serverfault.com/questions/715827/how-to-generate-key-and-crt-file-from-jks-file-for-httpd-apache-server
func JksToPem(jksPath, jksPassword string, overwrite bool) (certPemPath, keyPemPath string, err error) {
	dir, fn := filepath.Split(jksPath)
	certPemPath = filepath.Join(dir, fn+".cert.pem")
	keyPemPath = filepath.Join(dir, fn+".key.pem")
	pkcs12Path := filepath.Join(dir, fn+".p12")
	if overwrite {
		for _, fp := range []string{certPemPath, keyPemPath, pkcs12Path} {
			if err = os.RemoveAll(fp); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	} else {
		for _, fp := range []string{certPemPath, keyPemPath, pkcs12Path} {
			if _, err = os.Stat(fp); err == nil {
				return
			}
		}
	}
	cmds := [][]string{
		{"keytool", "-importkeystore", "-srckeystore", jksPath, "-destkeystore", pkcs12Path, "-deststoretype", "PKCS12"},
		{"openssl", "pkcs12", "-in", pkcs12Path, "-nokeys", "-out", certPemPath, "-passin", "env:password"},
		{"openssl", "pkcs12", "-in", pkcs12Path, "-nodes", "-nocerts", "-out", keyPemPath, "-passin", "env:password"},
	}
	for _, cmd := range cmds {
		klog.Infof(strings.Join(cmd, " "))
		exe := exec.Command(cmd[0], cmd[1:]...)
		if cmd[0] == "keytool" {
			exe.Stdin = bytes.NewReader([]byte(jksPassword + "\n" + jksPassword + "\n" + jksPassword))
		} else if cmd[0] == "openssl" {
			exe.Env = []string{fmt.Sprintf("password=%s", jksPassword)}
		}
		var out []byte
		out, err = exe.CombinedOutput()
		klog.Infof(string(out))
		if err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}
