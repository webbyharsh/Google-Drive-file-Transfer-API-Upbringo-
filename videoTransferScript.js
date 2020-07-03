var { google } = require('googleapis');
var drive = require("../api/resolver/Common/drive");
const argv = require('yargs').argv
const timestamp = Date.now();
const winston = require('winston');
var getParentsEmailIdForClass = require("../api/resolver/Common/helper").getParentsEmailIdForClass

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  defaultMeta: { service: 'user-service' },
  transports: [
    //
    // - Write all logs with level `error` and below to `error.log`
    // - Write all logs with level `info` and below to `combined.log`
    //
    new winston.transports.File({ filename: timestamp + 'error.log', level: 'error' }),
    new winston.transports.File({ filename: timestamp + 'combined.log' }),
  ],
});


let localKey
let globalKey
let localAdmin
let globalAdmin
let localDrive
let globalDrive


let looping_interval = argv.interval * 1000;
const client = require('../api/resolver/Common/db').getClient()  //db client

let localDriveParentId;
let global_filename
let global_videoId
let original_video_id
let newParentsInGlobalAdminDrive
let parentsEmail = [];


async function initialize ( school_id, client ){
    
    localDrive = await drive.localGoogleAuthJwtApi( client, school_id, true );
    globalDrive = await drive.globalGoogleAuthJwtApi( client, school_id );
    
}
//VIDEO TRANSFER FROM localDrive =====> globalDrive

const schoolsFromDb = await client.query(`SELECT school_id
FROM school`);
main();
async function main() {
    for( schoolIndex in schoolsFromDb ) {
      let school_id = schooldFromDb[ schoolIndex ].school_id;
      await initialize( school_id, client );
      await checkForFiles();
    }
}


async function checkForFiles(){
    try {
        let filesFromLocalDrive = await localDrive.files.list({ 
            q: "mimeType = 'application/vnd.google-apps.folder' and name = 'Meet Recordings' ",   //query to get Meet Recordings folder
            fields: 'files(id, name)'
        });
        if( filesFromLocalDrive.data.files.length > 0 ){
            let files = filesFromLocalDrive.data.files
            logger.info( files );
            localDriveParentId = files[ 0 ].id;
            logger.info( "The parent drive ID " + localDriveParentId +" and name is " + files[ 0 ].name );
            await getOriginalVideoId();
        } else {
            logger.info( "Meet Recordings Folder doesn't exists" );
        }
        
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: checkForFiles", err );
    }
}

async function getSubFolderIdForVideoName( file_name ){
    let tc_event_id = file_name.split(" ")[ 0 ];
    let subFolderId
    try {
        if( !file_name ){
            throw Error( "File name cannot be null " );
        }
        let subFolderFromDb = await client.query(`SELECT cs.drive_identifier 
        FROM class_school as cs
        INNER JOIN timetable_class as tc
        ON tc.class_id = cs.class_id
        where tc.event_id = ?`, [
            tc_event_id
        ]);
        if( subFolderFromDb.length ){
            subFolderId = subFolderFromDb.drive_identifier;
        }
    } catch ( err ) {
        console.log( "GLOBAL ERROR: getSubFolderIdForVideoName", err );
    } finally {
        return subFolderId
    }

}
async function shareWithParentsForClass( client, file_name, folder_id ){
    let tc_event_id = file_name.split(" ")[ 0 ];
    let class_id;
    try {
        let classFromDb = await client.query(`SELECT class_id FROM
        timetable_class WHERE event_id = ?`, [ tc_event_id ]);
        if( classFromDb.length ){
            class_id = classFromDb[ 0 ].class_id;
        }
        parentsEmail = await getParentsEmailIdForClass( client, class_id ); 
        await drive.shareWriteableFolderWithPersons( globalDrive, folder_id, parentsEmail, school_id, client );
    } catch ( err ) {
        console.log( "GLOBAL ERROR: shareWithParentsForClass" );
    } 
}
async function getOriginalVideoId() {
    try {
        let localFilesList = await localDrive.files.list({
            pageSize: 1000,
            q: "'"+ localDriveParentId+"' in parents",
            fields: 'files(id, name)'
        });
        const files = localFilesList.data.files;
    
        if( files.length ){
            logger.info( `File found with name = ${ files[ 0 ].name } and ID = ${ files[ 0 ].id }` );
            global_filename = files[ 0 ].name; 
            newParentsInGlobalAdminDrive = await getSubFolderIdForVideoName( global_filename );
            await checkSharedFile( global_filename );
        } else {
            logger.info( "No files found in Meet Recordings folder" );
        }    
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: getOriginalVideoId", err );
    }
    
}


async function checkSharedFile( file_name ) {
    try {
        let filesList = await globalDrive.files.list({
            q: "name='" + file_name + "'",
            pageSize: 1000,
            fields: 'files(id, name)'
        });
        let files = filesList.data.files;
        if( files.length ){
            //let file = files.find( file => file.name == file_name );
            let file = files[ 0 ];
            original_video_id = file.id;
            logger.info( "Copying original video..." );
            await copyOriginalVideo( original_video_id );
        }    
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: checkSharedFile", err );
    }
    
}

async function copyOriginalVideo( file_id ) {
    try {
        let copyVideo = await globalDrive.files.copy({
            fileId: file_id,
            fields: `id`,
            requestBody:{
                copyRequiresWriterPermission: true
            }
        });    
        global_videoId = copyVideo.data.id;
        logger.info( "Original Video copied with video ID " + copyVideo.data.id );
        await moveVideo( global_videoId );
        await deleteOriginalVideo( file_id )
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: copyOrignalVideo", err );
    }

}


async function deleteOriginalVideo( video_id ){
    try {
        await localDrive.files.delete({
            fileId: video_id
        });
        logger.info( "Original Video Deleted" );
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: deleteOriginalVideo", err );
    }
}
async function moveVideo ( file_id ){
    let oldParents;
    try {
        let fileParents = await globalDrive.files.get({
            fileId: file_id,
            fields: `parents`
        });
        oldParents = fileParents.data;
    
        let updateParents = await globalDrive.files.update({
            fileId: file_id,
            fields: `id, parents`,
            addParents: newParentsInGlobalAdminDrive,
            removeParents: oldParents
        });

        logger.info( "File moved to Meet Recordings" ); 
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: moveVideo", err );
    } finally {
        let event_id = global_filename.split(" ")[ 0 ];   //TO GET tc_event_id FOLLOWING THE CONVENTION 'XXXXXXXXXXX (2020-0X-XX)
        let file_id = global_videoId;
        await saveLinkInTimetableEvent( event_id, file_id );
        await shareWithParentsForClass( client, global_filename, newParentsInGlobalAdminDrive )
    }
}

async function saveLinkInTimetableEvent( event_id, fileId ){
    try {
        let eventIdFromDb = await client.query(`SELECT event_id,
        tc_event_id, link 
        FROM timetable_event 
        WHERE tc_event_id = ?
        ORDER BY datetime 
        DESC LIMIT 1`, [ event_id ]);
        let te_event_id = eventIdFromDb[ 0 ].event_id;

        if( eventIdFromDb[ 0 ].link == NULL ){
            logger.info(" MOST RECENT TE_EVENT_ID --->" + te_event_id);
            let insertDataInDb = await client.query(`UPDATE timetable_event
            SET link = ? 
            WHERE event_id = ? `, [ 
                fileId, 
                te_event_id 
            ]);
        }
            
    } catch ( err ) {
        logger.error( "GLOBAL ERROR: saveLinkInTimetableEvent", err );
    } 

}
