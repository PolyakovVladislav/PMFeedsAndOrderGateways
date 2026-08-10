package dev.poliakov.pmFeedsAndOrderGateways.infrastructure.polymarket.gateway;

import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.Hash;
import org.web3j.crypto.Keys;
import org.web3j.utils.Numeric;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

public final class Eip712Utils {

    private Eip712Utils() {
    }

    static byte[] keccak256(String input) {
        return keccak256(input.getBytes(StandardCharsets.UTF_8));
    }

    static byte[] keccak256(byte[] input) {
        return Hash.sha3(input);
    }

    static byte[] toUint256(BigInteger value) {
        return Numeric.toBytesPadded(value, 32);
    }

    static byte[] toAddress(String hexAddress) {
        String clean = hexAddress.startsWith("0x") ? hexAddress.substring(2) : hexAddress;
        return Numeric.toBytesPadded(new BigInteger(clean, 16), 32);
    }

    public static String deriveAddress(String privateKeyHex) {
        String hex = privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex;
        ECKeyPair keyPair = ECKeyPair.create(new BigInteger(hex, 16));
        return Keys.toChecksumAddress(Numeric.prependHexPrefix(Keys.getAddress(keyPair)));
    }
}

