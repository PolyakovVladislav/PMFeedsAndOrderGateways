package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Sign;
import org.web3j.utils.Numeric;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.Eip712Utils.*;

public class Eip712AuthSigner {

    private static final long CHAIN_ID = 137L;
    private static final String MESSAGE = "This message attests that I control the given wallet";

    private static final byte[] DOMAIN_TYPE_HASH =
            keccak256("EIP712Domain(string name,string version,uint256 chainId)");
    private static final byte[] DOMAIN_NAME_HASH = keccak256("ClobAuthDomain");
    private static final byte[] DOMAIN_VERSION_HASH = keccak256("1");

    private static final byte[] AUTH_TYPE_HASH =
            keccak256("ClobAuth(address address,string timestamp,uint256 nonce,string message)");
    private static final byte[] MESSAGE_HASH = keccak256(MESSAGE);

    private final byte[] domainSeparator;
    private final ECKeyPair keyPair;
    private final String address;

    public Eip712AuthSigner(String privateKeyHex) {
        String hex = privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex;
        this.keyPair = ECKeyPair.create(new BigInteger(hex, 16));
        this.address = deriveAddress(privateKeyHex);
        this.domainSeparator = keccak256(ByteBuffer.allocate(4 * 32)
                .put(DOMAIN_TYPE_HASH)
                .put(DOMAIN_NAME_HASH)
                .put(DOMAIN_VERSION_HASH)
                .put(toUint256(BigInteger.valueOf(CHAIN_ID)))
                .array());
    }

    public String address() {
        return address;
    }

    public String sign(long timestampSeconds, long nonce) {
        byte[] structHash = keccak256(ByteBuffer.allocate(5 * 32)
                .put(AUTH_TYPE_HASH)
                .put(toAddress(address))
                .put(keccak256(Long.toString(timestampSeconds)))
                .put(toUint256(BigInteger.valueOf(nonce)))
                .put(MESSAGE_HASH)
                .array());

        byte[] digest = keccak256(ByteBuffer.allocate(66)
                .put((byte) 0x19).put((byte) 0x01)
                .put(domainSeparator)
                .put(structHash)
                .array());

        Sign.SignatureData sig = Sign.signMessage(digest, keyPair, false);
        byte[] signature = new byte[65];
        System.arraycopy(sig.getR(), 0, signature, 0, 32);
        System.arraycopy(sig.getS(), 0, signature, 32, 32);
        signature[64] = sig.getV()[0];
        return Numeric.toHexString(signature);
    }
}

