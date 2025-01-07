package com.starrocks.plugin.audit;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DecryptUtil {

    public static String decrypt(String message, byte[] secretKey) throws Exception {
        SecretKeySpec key = new SecretKeySpec(secretKey, "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] encryptedBytes = Base64.getDecoder().decode(message);
        byte[] decryptedBytes = cipher.doFinal(encryptedBytes);
        return new String(decryptedBytes);
    }

    /**
     * 对密钥进行填充
     * @param bytes
     * @param length
     * @return
     */
    public static byte[] fillByte(byte[] bytes, int length) {
        byte[] res = new byte[length];
        for (int i = 0; i < length; i++) {
            if (i < bytes.length) {
                res[i] = bytes[i];
            } else {
                res[i] = 0;
            }
        }
        return res;
    }

    /**
     * 判断密钥是否为16进制字符串
     * @param str
     * @return
     */
    private boolean isHexStr(String str) {
        // 定义十六进制字符串的正则表达式模式
        String pattern = "^[0-9a-fA-F]+$";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(str);
        return m.matches();
    }

    /**
     * 对十六进制密钥进行转换
     * @param str
     * @return
     */
    public byte[] unhex(String str) {
        byte[] res = new byte[str.length() / 2];
        for (int i = 0; i < str.length(); i += 2) {
            String subStr = str.substring(i, i + 2);
            int value = Integer.parseInt(subStr, 16);
            res[i/2] = (byte) value;
        }
        return res;
    }
}
