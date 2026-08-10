package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.kalshi;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.PSSParameterSpec;
import java.util.Base64;

public class RsaPssKalshiCredentials implements KalshiCredentials {

    private static final String PKCS1_MARKER = "BEGIN RSA PRIVATE KEY";

    private static final byte[] RSA_ALGORITHM_ID = {
            0x30, 0x0D, 0x06, 0x09, 0x2A, (byte) 0x86, 0x48, (byte) 0x86, (byte) 0xF7,
            0x0D, 0x01, 0x01, 0x01, 0x05, 0x00
    };

    private final String apiKeyId;
    private final String privateKeyPem;

    private volatile PrivateKey privateKey;

    public RsaPssKalshiCredentials(String apiKeyId, String privateKeyPem) {
        this.apiKeyId = apiKeyId;
        this.privateKeyPem = privateKeyPem;
    }

    private static PrivateKey parsePrivateKey(String pem) {
        boolean isPkcs1 = pem.contains(PKCS1_MARKER);
        String base64 = pem
                .replaceAll("-----BEGIN [^-]+-----", "")
                .replaceAll("-----END [^-]+-----", "")
                .replaceAll("\\s", "");
        byte[] decoded = Base64.getDecoder().decode(base64);
        byte[] pkcs8 = isPkcs1 ? wrapPkcs1(decoded) : decoded;
        try {
            KeyFactory factory = KeyFactory.getInstance("RSA");
            return factory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Kalshi RSA private key", e);
        }
    }

    private static byte[] wrapPkcs1(byte[] pkcs1) {
        byte[] octetString = derWrap(0x04, pkcs1);
        byte[] version = {0x02, 0x01, 0x00};
        byte[] body = concat(version, RSA_ALGORITHM_ID, octetString);
        return derWrap(0x30, body);
    }

    private static byte[] derWrap(int tag, byte[] content) {
        byte[] length = derLength(content.length);
        byte[] result = new byte[1 + length.length + content.length];
        result[0] = (byte) tag;
        System.arraycopy(length, 0, result, 1, length.length);
        System.arraycopy(content, 0, result, 1 + length.length, content.length);
        return result;
    }

    private static byte[] derLength(int len) {
        if (len < 0x80) {
            return new byte[]{(byte) len};
        }
        int numBytes = (32 - Integer.numberOfLeadingZeros(len) + 7) / 8;
        byte[] result = new byte[1 + numBytes];
        result[0] = (byte) (0x80 | numBytes);
        for (int i = numBytes; i >= 1; i--) {
            result[i] = (byte) (len & 0xFF);
            len >>= 8;
        }
        return result;
    }

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (byte[] a : arrays) {
            total += a.length;
        }
        byte[] result = new byte[total];
        int pos = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, result, pos, a.length);
            pos += a.length;
        }
        return result;
    }

    @Override
    public String apiKeyId() {
        return apiKeyId;
    }

    @Override
    public String sign(String message) {
        try {
            Signature signature = Signature.getInstance("RSASSA-PSS");
            signature.setParameter(new PSSParameterSpec(
                    "SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1));
            signature.initSign(resolve());
            signature.update(message.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(signature.sign());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to sign Kalshi request", e);
        }
    }

    private PrivateKey resolve() {
        PrivateKey existing = privateKey;
        if (existing != null) {
            return existing;
        }
        synchronized (this) {
            if (privateKey == null) {
                privateKey = parsePrivateKey(privateKeyPem);
            }
            return privateKey;
        }
    }
}

