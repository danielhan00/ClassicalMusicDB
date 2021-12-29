const fetch = require('node-fetch');
const mysql = require('mysql');

const api = "https://api.deezer.com";

const connection = mysql.createConnection({
    host     : 'classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
    user     : 'admin',
    password : 'password',
    database : 'test'
});

connection.connect(async function(err) {
    if (err) throw err;
    console.log(`Connected as id ${connection.threadId}`);

    const res = await fetch(`${api}/chart/98`);
    const body = await res.json();

    const { tracks, albums, artists, playlists } = body;

    const promises = [];

    for (const track of tracks.data) {
        promises.push(processTrack(track.id));
    }

    for (const album of albums.data) {
        promises.push(processAlbum(album.id))
    }

    await Promise.all(promises);

    connection.end();
    process.exit(0);
});

const processAlbum = async (albumId) => {
    const albumRes = await fetch(`${api}/album/${albumId}`);
    const albumInfo = await albumRes.json();

    for (const track of albumInfo.tracks.data) {
        await processTrack(track.id);
    }
}

const processTrack = async (trackId) => {

    let trackInfo = null
    // Need to loop because we hit rate limiting
    while (trackInfo === null || trackInfo.error) {
        const trackRes = await fetch(`${api}/track/${trackId}`);
        trackInfo = await trackRes.json();
    }

    const songId = await insertSong(trackInfo.title)
    const recordingId = await insertRecording(songId, trackInfo.release_date);
    await insertRecordingSource(recordingId, trackInfo.id);

    // Split on one of: ", " " and " "/" (spaces included)
    const performers = trackInfo.artist.name.split(/, | and |\//);

    for (const performer of performers) {
        const performerId = await insertPerformer(performer);
        await insertRecordingPerformer(recordingId, performerId);
    }
}

const insertSong = (title) => {
    return new Promise((resolve, reject) => {
        connection.query(`INSERT INTO song (title) VALUES (?);`,[title], (err, result) => {
            if (err) return reject(err);
            resolve(result.insertId);
        });
    });
}

const insertPerformer = (name) => {
    return new Promise((resolve, reject) => {
        return connection.query(`SELECT performer_id FROM performer WHERE performer_name = ?;`, [name], (err, result) => {
            if (err) return reject(err);
            if (result.length === 0) {
                connection.query(`INSERT INTO performer (performer_name) VALUES (?);`,[name], (err, result) => {
                    if (err) return reject(err);
                    resolve(result.insertId);
                });
            } else {
                resolve(result[0].performer_id);
            }
        })
    });
}

const insertRecording = (songId, date) => {
    return new Promise((resolve, reject) => {
        return connection.query(`SELECT recording_id FROM recording WHERE song_id = ? AND recorded_date = ?;`,[songId, date], (err, result) => {
            if (err) return reject(err);
            if (result.length === 0) {
                connection.query(`INSERT INTO recording (song_id, recorded_date) VALUES (?, ?);`,[songId, date], (err, result) => {
                    if (err) reject(err);
                    resolve(result.insertId);
                });
            } else {
                resolve(result[0].recording_id);
            }
        })
    });
}

const insertRecordingPerformer = (recordingId, performerId) => {
    return new Promise((resolve, reject) => {
        connection.query(`INSERT INTO recording_performer (recording_id, performer_id) VALUES (?, ?);`, [recordingId, performerId], (err, result) => {
            if (err) return reject(err);
            resolve(result.insertId);
        });

    });
}

const insertRecordingSource = (recordingId, sourceSongId) => {
    return new Promise((resolve, reject) => {
        connection.query(`INSERT INTO recording_source (source_id, recording_id, source_song_id) VALUES (1, ?, ?);`, [recordingId, sourceSongId], (err, result) => {
            if (err) return reject(err);
            resolve(result.insertId);
        });

    });
}
