package de.hpi.dbs1.fixtures

import de.hpi.dbs1.entities.Actor

object Actors {
    @JvmField
    val ANNE_HATHAWAY = Actor("nm0004266", "Anne Hathaway").apply {
        playedIn.addAll(
            listOf(
                "Mothers' Instinct",
                "The Idea of You",
                "She Came to Me",
                "Armageddon Time",
                "Locked Down",
            )
        )
        costarNameToCount.putAll(
            mapOf(
                "Hector Elizondo" to 3,
                "Helena Bonham Carter" to 3,
                "Heather Matarazzo" to 2,
                "Jeremy Strong" to 2,
                "Jesse Eisenberg" to 2,
            )
        )
    }

    @JvmField
    val MORGAN_FREEMAN = Actor("nm0000151", "Morgan Freeman").apply {
        playedIn.addAll(
            listOf(
                "My Dead Friend Zoe",
                "57 Seconds",
                "A Good Person",
                "The Ritual Killer",
                "Paradise Highway",
            )
        )
        costarNameToCount.putAll(
            mapOf(
                "Michael Caine" to 5,
                "Aaron Eckhart" to 4,
                "Ashley Judd" to 4,
                "Bruce Willis" to 3,
                "Cary Elwes" to 3,
            )
        )
    }
    @JvmField
    val FREEMAN_WOOD = Actor("nm0939706", "Freeman Wood")
    @JvmField
    val KATHLEEN_FREEMAN = Actor("nm0293466", "Kathleen Freeman")
    @JvmField
    val HOWARD_FREEMAN = Actor("nm0293418", "Howard Freeman")
    @JvmField
    val MONA_FREEMAN = Actor("nm0293530", "Mona Freeman")
}
