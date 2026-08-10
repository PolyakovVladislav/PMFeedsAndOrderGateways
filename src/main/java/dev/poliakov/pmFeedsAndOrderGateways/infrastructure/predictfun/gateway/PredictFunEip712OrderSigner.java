package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.predictfun.gateway;

import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Hash;
import org.web3j.crypto.Sign;
import org.web3j.utils.Numeric;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class PredictFunEip712OrderSigner {

    private static final String PROTOCOL_NAME = "predict.fun CTF Exchange";
    private static final String PROTOCOL_VERSION = "1";

    private static final String ORDER_TYPE_STRING =
            "Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,"
                    + "uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,"
                    + "uint256 feeRateBps,uint8 side,uint8 signatureType)";

    private static final byte[] ORDER_TYPE_HASH = keccak256(ORDER_TYPE_STRING);
    private static final byte[] DOMAIN_TYPE_HASH = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    private static final byte[] NAME_HASH = keccak256(PROTOCOL_NAME);
    private static final byte[] VERSION_HASH = keccak256(PROTOCOL_VERSION);

    private final ECKeyPair keyPair;
    private final long chainId;

    PredictFunEip712OrderSigner(String privateKeyHex, long chainId) {
        String hex = privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex;
        this.keyPair = ECKeyPair.create(new BigInteger(hex, 16));
        this.chainId = chainId;
    }

    private static byte[] keccak256(String input) {
        return Hash.sha3(input.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] keccak256(byte[] input) {
        return Hash.sha3(input);
    }

    private static byte[] toUint256(BigInteger value) {
        return Numeric.toBytesPadded(value, 32);
    }

    private static byte[] toAddress(String hexAddress) {
        String clean = hexAddress.startsWith("0x") ? hexAddress.substring(2) : hexAddress;
        return Numeric.toBytesPadded(new BigInteger(clean, 16), 32);
    }

    String sign(String verifyingContract, BigInteger salt, String maker, String signer, String taker,
                String tokenId, BigInteger makerAmount, BigInteger takerAmount,
                long expiration, BigInteger nonce, int feeRateBps, int side, int signatureType) {

        byte[] structHash = keccak256(ByteBuffer.allocate(13 * 32)
                .put(ORDER_TYPE_HASH)
                .put(toUint256(salt))
                .put(toAddress(maker))
                .put(toAddress(signer))
                .put(toAddress(taker))
                .put(toUint256(new BigInteger(tokenId)))
                .put(toUint256(makerAmount))
                .put(toUint256(takerAmount))
                .put(toUint256(BigInteger.valueOf(expiration)))
                .put(toUint256(nonce))
                .put(toUint256(BigInteger.valueOf(feeRateBps)))
                .put(toUint256(BigInteger.valueOf(side)))
                .put(toUint256(BigInteger.valueOf(signatureType)))
                .array());

        byte[] digest = keccak256(ByteBuffer.allocate(66)
                .put((byte) 0x19).put((byte) 0x01)
                .put(domainSeparator(verifyingContract))
                .put(structHash)
                .array());

        Sign.SignatureData sig = Sign.signMessage(digest, keyPair, false);
        byte[] signature = new byte[65];
        System.arraycopy(sig.getR(), 0, signature, 0, 32);
        System.arraycopy(sig.getS(), 0, signature, 32, 32);
        signature[64] = sig.getV()[0];
        return Numeric.toHexString(signature);
    }

    private byte[] domainSeparator(String verifyingContract) {
        return keccak256(ByteBuffer.allocate(5 * 32)
                .put(DOMAIN_TYPE_HASH)
                .put(NAME_HASH)
                .put(VERSION_HASH)
                .put(toUint256(BigInteger.valueOf(chainId)))
                .put(toAddress(verifyingContract))
                .array());
    }
}

