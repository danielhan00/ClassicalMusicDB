const mysql = require('mysql');

const connection = mysql.createConnection({
    host     : 'classically.c986fzuamnpo.us-east-1.rds.amazonaws.com',
    user     : 'admin',
    password : 'password',
    database : 'test',
    multipleStatements: true
});

connection.connect(async function(err) {
    if (err) throw err;
    console.log(`Connected as id ${connection.threadId}`);

    const duplicateComposers = await getComposerDuplicates();
    const composerPromises = []
    for (const composer of duplicateComposers) {
        composerPromises.push(dedupeComposer(composer.composer_name));
    }
    await Promise.all(composerPromises);
    console.log(`Deduped ${composerPromises.length} composers`);

    const duplicatePerformers = await getPerformerDuplicates();
    const performerPromises = [];
    for (const performer of duplicatePerformers) {
        performerPromises.push(dedupePerformer(performer.performer_name));
    }
    await Promise.all(performerPromises);
    console.log(`Deduped ${performerPromises.length} performers`);

    const duplicateSongs = await getSongDuplicates();
    const songPromises = [];
    for (const song of duplicateSongs) {
        songPromises.push(dedupeSong(song.title));
    }
    await Promise.all(songPromises);
    console.log(`Deduped ${songPromises.length} songs`);

    const duplicateRecordings = await getRecordingDuplicates();
    const recordingPromises = [];
    for (const recording of duplicateRecordings) {
        recordingPromises.push(dedupeRecording(recording.song_id))
    }
    await Promise.all(recordingPromises);
    console.log(`Deduped ${recordingPromises.length} recordings`);

    const duplicateRecordingSources = await getRecordingSourceDuplicates();
    const recordingSourcePromises = [];
    for (const source of duplicateRecordingSources) {
        recordingSourcePromises.push(deleteDupeRecordingSource(source));
    }
    await Promise.all(recordingSourcePromises);
    console.log(`Deduped ${recordingSourcePromises.length} recording sources`);

    connection.end();
    process.exit(0);
});

const dedupeComposer = (name) => {
    return new Promise((resolve, reject) => {
        connection.beginTransaction(async (err) => {
            if (err) {
                console.log('failed to transact');
                return reject(connection.rollback());
            }

            try {
                const rows = await getComposerRows(name);
                const keepRow = rows[0].composer_id;
                rows.shift();

                const oldRows = rows.map((r) => r.composer_id);

                await updateSongs(keepRow, oldRows);
                await deleteDupeComposers(oldRows);

                connection.commit(function(err) {
                    if (err) {
                        console.log('failed to commit');
                        return reject(connection.rollback());
                    }

                    resolve();
                });
            } catch (e) {
                console.log(e);
                return reject(connection.rollback());
            }
        });
    });
}

const dedupePerformer = (name) => {
    return new Promise((resolve, reject) => {
        connection.beginTransaction(async (err) => {
            if (err) {
                console.log('failed to transact');
                return reject(connection.rollback());
            }

            try {
                const rows = await getPerformerRows(name);
                const keepRow = rows[0].performer_id;
                rows.shift();

                const oldRows = rows.map((r) => r.performer_id);

                await updateRecordingPerformer(keepRow, oldRows);
                await deleteDupePerformers(oldRows);

                connection.commit(function(err) {
                    if (err) {
                        console.log('failed to commit');
                        return reject(connection.rollback());
                    }

                    resolve();
                });
            } catch (e) {
                console.log(e);
                return reject(connection.rollback());
            }
        });
    });
}

const dedupeSong = (title) => {
    return new Promise((resolve, reject) => {
        connection.beginTransaction(async (err) => {
            if (err) {
                console.log('failed to transact');
                return reject(connection.rollback());
            }

            try {
                let rows = await getSongRows(title);
                const keepRow = (rows.find(row => row.composer_id !== null) || rows[0]).song_id;
                rows = rows.filter(row => row.song_id !== keepRow);

                const oldRows = rows.map((r) => r.song_id);

                await updateRecording(keepRow, oldRows);
                await deleteDupeSong(oldRows);

                connection.commit(function(err) {
                    if (err) {
                        console.log('failed to commit');
                        return reject(connection.rollback());
                    }

                    resolve();
                });
            } catch (e) {
                console.log(e);
                return reject(connection.rollback());
            }
        });
    });
}


const dedupeRecording = (song_id) => {
    return new Promise((resolve, reject) => {
        connection.beginTransaction(async (err) => {
            if (err) {
                console.log('failed to transact');
                connection.rollback(() => {
                    return reject(err);
                });
            } else {
                try {
                    const rows = await getRecordingRows(song_id);
                    // Rows with most performers will be first
                    rows.sort((a, b) => b.performers.length - a.performers.length);

                    let seenIds = [];
                    for (let i = 0; i < rows.length; i++) {
                        const row = rows[i];

                        if (seenIds.includes(row.recording_id)) {
                            continue;
                        }

                        // Other rows that have a subset of the performers of this row
                        const samePerformers = rows.filter((other) =>
                            other.performers.every(p => row.performers.includes(p))
                        );

                        samePerformers.concat([row]).sort((a, b) => (a.recorded_date || "").localeCompare(b.recorded_date || ""))

                        if (samePerformers.length === 1) {
                            continue;
                        }

                        const keepRow = samePerformers[0].recording_id;
                        samePerformers.shift();
                        const oldRows = samePerformers.map((r) => r.recording_id);

                        await updateRecordingJoinTables(keepRow, oldRows);
                        await deleteDupeRecording(oldRows);

                        seenIds = seenIds.concat(samePerformers.map((r) => r.recording_id));
                    }

                    connection.commit(function (err) {
                        if (err) {
                            console.log('failed to commit');
                            connection.rollback(() => {
                                return reject(err);
                            });
                        } else {
                            resolve();
                        }
                    });
                } catch (e) {
                    console.log(e);
                    connection.rollback(() => {
                        return reject(err);
                    });
                }
            }
        });
    });
}

const getComposerDuplicates = () => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT COUNT(*) AS count, composer_name FROM composer GROUP BY composer_name HAVING count > 1;`,(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getPerformerDuplicates = () => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT COUNT(*) AS count, performer_name FROM performer GROUP BY performer_name HAVING count > 1;`,(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getSongDuplicates = () => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT COUNT(*) AS count, title FROM song GROUP BY title HAVING count > 1;`,(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getRecordingDuplicates = () => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT COUNT(*) AS count, song_id FROM recording GROUP BY song_id HAVING count > 1;`,(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getRecordingSourceDuplicates = () => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT *, COUNT(*) AS count FROM recording_source GROUP BY recording_id, source_id, source_song_id HAVING COUNT(*) > 1;`,(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getComposerRows = (name) => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT * FROM composer WHERE composer_name = ?;`, [name],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getPerformerRows = (name) => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT * FROM performer WHERE performer_name = ?;`, [name],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getSongRows = (title) => {
    return new Promise((resolve, reject) => {
        connection.query(`SELECT * FROM song WHERE title = ?;`, [title],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const getRecordingRows = (songId) => {
    return new Promise((resolve, reject) => {
        connection.query(`
SELECT recording_id, DATE_FORMAT(recorded_date, "%Y-%m-%e") AS recorded_date, GROUP_CONCAT(performer_name) AS performers FROM recording
JOIN recording_performer USING (recording_id)
JOIN performer USING (performer_id) 
JOIN recording_source USING (recording_id)
WHERE song_id = ?
GROUP BY recording_id, song_id;
`, [songId],(err, result) => {
            if (err) return reject(err);
            result.forEach((r) => {
                r.performers = r.performers.split(',');
            })
            resolve(result);
        });
    });
}

const updateSongs = (newId, oldIds) => {
    return new Promise((resolve, reject) => {
        connection.query(`UPDATE song SET composer_id = ? WHERE composer_id IN (?);`, [newId, oldIds],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const updateRecordingPerformer = (newId, oldIds) => {
    return new Promise((resolve, reject) => {
        connection.query(`UPDATE recording_performer SET performer_id = ? WHERE performer_id IN (?);`, [newId, oldIds],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const updateRecording = (newId, oldIds) => {
    return new Promise((resolve, reject) => {
        connection.query(`UPDATE recording SET song_id = ? WHERE song_id IN (?);`, [newId, oldIds],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const updateRecordingJoinTables = (newId, oldIds) => {
    const allIds = oldIds.concat([newId]);
    return new Promise((resolve, reject) => {
        connection.query(
`SELECT DISTINCT performer_id FROM recording_performer WHERE recording_id IN (?);`,
            [allIds],(err, result) => {
            if (err) return reject(err);

            connection.query(
`DELETE FROM recording_performer WHERE recording_id IN (?);
INSERT INTO recording_performer (recording_id, performer_id) VALUES ${result.map((r) => `(${newId}, ${r.performer_id})`).join(',')}`
                , [allIds],(err, result) => {
                if (err) return reject(err);

                    connection.query(`
UPDATE recording_source SET recording_id = ? WHERE recording_id IN (?);
UPDATE popularity_measure SET recording_id = ? WHERE recording_id IN (?);`,
                        [newId, oldIds, newId, oldIds],(err, result) => {
                        if (err) return reject(err);
                        resolve(result);
                    });
            });

        });
    });
}

const deleteDupeComposers = (ids) => {
    return new Promise((resolve, reject) => {
        connection.query(`DELETE FROM composer WHERE composer_id IN (?);`, [ids],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const deleteDupePerformers = (ids) => {
    return new Promise((resolve, reject) => {
        connection.query(`DELETE FROM performer WHERE performer_id IN (?);`, [ids],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const deleteDupeSong = (ids) => {
    return new Promise((resolve, reject) => {
        connection.query(`DELETE FROM song WHERE song_id IN (?);`, [ids],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const deleteDupeRecording = (ids) => {
    return new Promise((resolve, reject) => {
        connection.query(`DELETE FROM recording WHERE recording_id IN (?);`, [ids],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

const deleteDupeRecordingSource = ({recording_id, source_id, source_song_id, count}) => {
    return new Promise((resolve, reject) => {
        connection.query(
`DELETE FROM recording_source WHERE recording_id = ? AND source_id = ? AND source_song_id = ? LIMIT ?;`,
            [recording_id, source_id, source_song_id, count - 1],(err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}