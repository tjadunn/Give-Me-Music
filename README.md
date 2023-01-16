# Give-Me-Music

Scala data pipeline which aggregates the music contained in youtube links from tweets on a Kafka topic

Service fingerprints each link and returns song and artist names contained within

E.g. Tweet read from topic `@SomeRandomPerson: "omg this tune is too good http://www.youtube.com/blahSickTunerightHere"`

Returns

`
{
    "kind": "youtube#videoListResponse",
    "etag": "NotImplemented",
    "items": [
        {
            "kind": "youtube#video",
            "etag": "NotImplemented",
            "id": "9W8B6uD7RnI",
            "musics": [
                {
                    "song": {
                        "title": "Torpedo",
                        "videoId": null
                    },
                    "artists": [
                        {
                            "title": "Skillibeng",
                            "channelId": "UCefBhh70GxGOi89otNmji_Q"
                        }
                    ],
                    "album": "Wide Spread Riddim",
                    "writers": null,
                    "licenses": "The Orchard Music (on behalf of Lone Don Entertainment); ASCAP, SOLAR Music Rights Management, BMI - Broadcast Music Inc., CMRRA, LatinAutorPerf, Sony Music Publishing, LatinAutor - SonyATV, UNIAO BRASILEIRA DE EDITORAS DE MUSICA - UBEM, and 4 Music Rights Societies"
                }
            ]
        }
    ]
}
`
