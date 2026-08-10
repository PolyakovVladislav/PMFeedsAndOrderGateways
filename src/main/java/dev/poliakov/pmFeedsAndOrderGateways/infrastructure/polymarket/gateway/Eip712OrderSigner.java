package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Sign;
import org.web3j.utils.Numeric;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway.Eip712Utils.*;

class Eip712OrderSigner {

    private static final long CHAIN_ID = 137L;

    private static final String ORDER_TYPE_STRING =
            "Order(uint256 salt,address maker,address signer,uint256 tokenId," +
                    "uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType," +
                    "uint256 timestamp,bytes32 metadata,bytes32 builder)";

    private static final String SOLADY_TYPE_STRING =
            "TypedDataSign(Order contents,string name,string version,uint256 chainId," +
                    "address verifyingContract,bytes32 salt)" +
                    ORDER_TYPE_STRING;
    private static final byte[] SOLADY_TYPE_HASH = keccak256(SOLADY_TYPE_STRING);
    private static final byte[] ORDER_TYPE_HASH = keccak256(ORDER_TYPE_STRING);
    private static final byte[] DOMAIN_TYPE_HASH = keccak256(
            "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");

    private static final byte[] DEPOSIT_WALLET_NAME_HASH = keccak256("DepositWallet");
    private static final byte[] DEPOSIT_WALLET_VERSION_HASH = keccak256("1");
    private static final byte[] DEPOSIT_WALLET_DOMAIN_SALT = new byte[32];

    private static final byte[] CTF_NAME_HASH = keccak256("Polymarket CTF Exchange");
    private static final byte[] CTF_VERSION_HASH = keccak256("2");

    private final byte[] appDomainSeparator;
    private final ECKeyPair keyPair;

    Eip712OrderSigner(String privateKeyHex, String contractAddress) {
        String hex = privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex;
        this.keyPair = ECKeyPair.create(new BigInteger(hex, 16));
        this.appDomainSeparator = buildAppDomainSeparator(contractAddress);
    }

    String sign(long salt, String maker, String signer, String tokenId,
                BigInteger makerAmount, BigInteger takerAmount,
                long timestamp, int side, int signatureType) {

        byte[] contentsHash = buildContentsHash(
                salt, maker, signer, tokenId,
                makerAmount, takerAmount, timestamp, side, signatureType);

        byte[] innerHash = buildTypedDataSignStructHash(contentsHash, signer);

        byte[] digest = keccak256(ByteBuffer.allocate(66)
                .put((byte) 0x19).put((byte) 0x01)
                .put(appDomainSeparator)
                .put(innerHash)
                .array());

        Sign.SignatureData sig = Sign.signMessage(digest, keyPair, false);

        byte[] innerSig = new byte[65];
        System.arraycopy(sig.getR(), 0, innerSig, 0, 32);
        System.arraycopy(sig.getS(), 0, innerSig, 32, 32);
        innerSig[64] = sig.getV()[0];

        byte[] orderTypeBytes = ORDER_TYPE_STRING.getBytes(StandardCharsets.UTF_8);
        int orderTypeLen = orderTypeBytes.length;
        return Numeric.toHexString(ByteBuffer
                .allocate(65 + 32 + 32 + orderTypeLen + 2)
                .put(innerSig)
                .put(appDomainSeparator)
                .put(contentsHash)
                .put(orderTypeBytes)
                .put((byte) (orderTypeLen >> 8))
                .put((byte) (orderTypeLen & 0xFF))
                .array());
    }

    private byte[] buildContentsHash(long salt, String maker, String signer, String tokenId,
                                     BigInteger makerAmount, BigInteger takerAmount,
                                     long timestamp, int side, int signatureType) {
        return keccak256(ByteBuffer.allocate(12 * 32)
                .put(ORDER_TYPE_HASH)
                .put(toUint256(BigInteger.valueOf(salt)))
                .put(toAddress(maker))
                .put(toAddress(signer))
                .put(toUint256(new BigInteger(tokenId)))
                .put(toUint256(makerAmount))
                .put(toUint256(takerAmount))
                .put(toUint256(BigInteger.valueOf(side)))
                .put(toUint256(BigInteger.valueOf(signatureType)))
                .put(toUint256(BigInteger.valueOf(timestamp)))
                .put(new byte[32])
                .put(new byte[32])
                .array());
    }

    private byte[] buildTypedDataSignStructHash(byte[] contentsHash, String signerAddress) {
        return keccak256(ByteBuffer.allocate(7 * 32)
                .put(SOLADY_TYPE_HASH)
                .put(contentsHash)
                .put(DEPOSIT_WALLET_NAME_HASH)
                .put(DEPOSIT_WALLET_VERSION_HASH)
                .put(toUint256(BigInteger.valueOf(CHAIN_ID)))
                .put(toAddress(signerAddress))
                .put(DEPOSIT_WALLET_DOMAIN_SALT)
                .array());
    }

    private byte[] buildAppDomainSeparator(String contractAddress) {
        return keccak256(ByteBuffer.allocate(5 * 32)
                .put(DOMAIN_TYPE_HASH)
                .put(CTF_NAME_HASH)
                .put(CTF_VERSION_HASH)
                .put(toUint256(BigInteger.valueOf(CHAIN_ID)))
                .put(toAddress(contractAddress))
                .array());
    }
}

