package net.corda.node.utilities

import com.google.common.util.concurrent.SettableFuture
import net.corda.testing.node.makeTestDataSourceProperties
import org.assertj.core.api.Assertions.assertThat
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.junit.Test
import rx.Observable
import rx.subjects.PublishSubject

class ObservablesTests {

    @Test
    fun `afterCommit delays until outside transaction again`() {
        val (toBeClosed, database) = configureDatabase(makeTestDataSourceProperties())

        val subject = PublishSubject.create<Unit>()
        val undelayedObservable: Observable<Unit> = subject
        val delayedObservable: Observable<Unit> = subject.afterDatabaseCommit()

        val delayedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()
        val undelayedEventSeqNo = SettableFuture.create<Pair<Int, Boolean>>()

        // Setup listeners and then send observation.
        var value = 0
        delayedObservable.subscribe { delayedEventSeqNo.set(value++ to isInDatabaseTransaction()) }
        undelayedObservable!!.subscribe { undelayedEventSeqNo.set(value++ to isInDatabaseTransaction()) }

        assertThat(subject).isNotEqualTo(delayedObservable)
        assertThat(subject).isEqualTo(undelayedObservable)

        databaseTransaction(database) {
            subject.onNext(Unit)
        }

        // Undelayed should fire first and be inside the transaction.
        assertThat(undelayedEventSeqNo.get()).isEqualTo(0 to true)
        // delayed should fire second and be outside the transaction.
        assertThat(delayedEventSeqNo.get()).isEqualTo(1 to false)

        toBeClosed.close()
    }

    private fun isInDatabaseTransaction(): Boolean = (TransactionManager.currentOrNull() != null)
}