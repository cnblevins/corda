package net.corda.core.crypto

import net.corda.core.crypto.PublicKeyTree.Leaf
import net.corda.core.crypto.PublicKeyTree.Node
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import java.security.PublicKey

/**
 * A tree data structure that enables the representation of composite public keys.
 *
 * In the simplest case it may just contain a single node encapsulating a [PublicKey] – a [Leaf].
 *
 * For more complex scenarios, such as *"Both Alice and Bob need to sign to consume a state S"*, we can represent
 * the requirement by creating a tree with a root [Node], and Alice and Bob as children – [Leaf]s.
 * The root node would specify *weights* for each of its children and a *threshold* – the minimum total weight required
 * (e.g. the minimum number of child signatures required) to satisfy the tree signature requirement.
 *
 * Using these constructs we can express e.g. 1 of N (OR) or N of N (AND) signature requirements. By nesting we can
 * create multi-level requirements such as *"either the CEO or 3 of 5 of his assistants need to sign"*.
 */
sealed class PublicKeyTree {
    /** Checks whether [keys] match a sufficient amount of leaf nodes */
    abstract fun isFulfilledBy(keys: Iterable<PublicKey>): Boolean

    fun isFulfilledBy(key: PublicKey) = isFulfilledBy(setOf(key))

    /** Returns all [PublicKey]s contained within the tree leaves */
    abstract val keys: Set<PublicKey>

    /** Checks whether any of the given [keys] matches a leaf on the tree */
    fun containsAny(otherKeys: Iterable<PublicKey>) = keys.intersect(otherKeys).isNotEmpty()

    // TODO: implement a proper encoding/decoding mechanism
    fun toBase58String(): String = Base58.encode(this.serialize().bits)

    companion object {
        fun parseFromBase58(encoded: String) = Base58.decode(encoded).deserialize<PublicKeyTree>()
    }

    /** The leaf node of the public key tree – a wrapper around a [PublicKey] primitive */
    class Leaf(val publicKey: PublicKey) : PublicKeyTree() {
        override fun isFulfilledBy(keys: Iterable<PublicKey>) = publicKey in keys

        override val keys: Set<PublicKey>
            get() = setOf(publicKey)

        // TODO: remove once data class inheritance is enabled
        override fun equals(other: Any?): Boolean {
            return this === other || other is Leaf && other.publicKey == this.publicKey
        }

        override fun hashCode() = publicKey.hashCode()

        override fun toString() = publicKey.toStringShort()
    }

    /**
     * Represents a node in the [PublicKeyTree]. It maintains a list of child nodes – sub-trees, and associated
     * [weights] carried by child node signatures.
     *
     * The [threshold] specifies the minimum total weight required (in the simple case – the minimum number of child
     * signatures required) to satisfy the public key sub-tree rooted at this node.
     */
    class Node(val threshold: Int,
               val children: List<PublicKeyTree>,
               val weights: List<Int>) : PublicKeyTree() {

        override fun isFulfilledBy(keys: Iterable<PublicKey>): Boolean {
            val totalWeight = children.mapIndexed { i, childNode ->
                if (childNode.isFulfilledBy(keys)) weights[i] else 0
            }.sum()

            return totalWeight >= threshold
        }

        override val keys: Set<PublicKey>
            get() = children.flatMap { it.keys }.toSet()

        // Auto-generated. TODO: remove once data class inheritance is enabled
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other?.javaClass != javaClass) return false

            other as Node

            if (threshold != other.threshold) return false
            if (weights != other.weights) return false
            if (children != other.children) return false

            return true
        }

        override fun hashCode(): Int {
            var result = threshold
            result = 31 * result + weights.hashCode()
            result = 31 * result + children.hashCode()
            return result
        }

        override fun toString() = "(${children.joinToString()})"
    }

    /** A helper class for building a [PublicKeyTree.Node]. */
    class Builder() {
        private val children: MutableList<PublicKeyTree> = mutableListOf()
        private val weights: MutableList<Int> = mutableListOf()

        /** Adds a child [PublicKeyTree] node. Specifying a [weight] for the child is optional and will default to 1. */
        fun addKey(publicKey: PublicKeyTree, weight: Int = 1): Builder {
            children.add(publicKey)
            weights.add(weight)
            return this
        }

        fun addKeys(vararg publicKeys: PublicKeyTree): Builder {
            publicKeys.forEach { addKey(it) }
            return this
        }

        fun addKeys(publicKeys: List<PublicKeyTree>): Builder = addKeys(*publicKeys.toTypedArray())

        /**
         * Builds the [PublicKeyTree.Node]. If [threshold] is not specified, it will default to
         * the size of the children, effectively generating an "N of N" requirement.
         */
        fun build(threshold: Int? = null): PublicKeyTree {
            return if (children.size == 1) children.first()
            else Node(threshold ?: children.size, children.toList(), weights.toList())
        }
    }

    /**
     * Returns the enclosed [PublicKey] for a [PublicKeyTree] with a single node
     *
     * @throws IllegalArgumentException if the [PublicKeyTree] contains more than one node
     */
    val singleKey: PublicKey
        get() = keys.singleOrNull() ?: throw IllegalStateException("The public key tree has more than one node")
}

/** Returns the set of all [PublicKey]s contained in the leaves of the [PublicKeyTree]s */
val Iterable<PublicKeyTree>.keys: Set<PublicKey>
    get() = flatMap { it.keys }.toSet()