/***
    ---------------------------------------------------------------------------

    Copyright (c) 2018 German Aerospace Center DLR e.V. (GSOC)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    This file is part of the openvocs project. https://openvocs.org

    ---------------------------------------------------------------------------
*//**
    @file           ov_webrtc.js

    @ingroup        ov_media

    @brief

    ---------------------------------------------------------------------------
*/
import * as ov_Vocs from "/lib/ov_vocs.js";
import ov_Audio from "./ov_audio.js";

export default class ov_WebRTC {
    static #local_stream;
    #inbound_stream;
    #outbound_stream;
    #peer_connection;

    #audio_element;

    #peer_configuration;

    #websocket;

    // --- NEW: outbound audio sender + tx tone state ---
    #audio_sender;
    #tx_tone_in_progress = false;
    #is_muted = true;   // logical mute state (for rising-edge detection)

    constructor(websocket, peer_configuration) {
        this.#websocket = websocket;
        this.#websocket.addEventListener(ov_Vocs.EVENT.CANDIDATE,
            this.#handle_ice_candidate.bind(this));
        this.#websocket.addEventListener(ov_Vocs.EVENT.END_OF_CANDIDATES,
            this.#handle_end_of_ice_candidates.bind(this));
        this.#websocket.addEventListener(ov_Vocs.EVENT.MEDIA_READY,
            this.#handle_media_ready.bind(this));
        this.#websocket.addEventListener(ov_Vocs.EVENT.MEDIA,
            this.#handle_media.bind(this));

        this.#peer_configuration = peer_configuration;
        if (!this.#peer_configuration)
            this.#peer_configuration = {
                iceServers: ICE_SERVERS
            };

        if (!ov_WebRTC.local_stream)
            console.error("Local stream needed to init ov_WebRTC. Please use ov_WebRTC.create_local_media_stream.");
        this.#outbound_stream = ov_Audio.clone(ov_WebRTC.#local_stream);

        this.set_up_peer_connection();
    }

    static get local_stream() {
        return ov_WebRTC.#local_stream;
    }

    get inbound_stream() {
        return this.#inbound_stream;
    }

    get outbound_stream() {
        return this.#outbound_stream;
    }

    get peer_connection() {
        return this.#peer_connection;
    }

    get stable_connection() {
        if (!this.#inbound_stream)
            return false;
        let tracks = this.#inbound_stream.getTracks();
        if (!tracks || !tracks[0])
            return false;
        return !tracks.muted;
    }

    get is_connected() {
        return this.#peer_connection && this.#peer_connection.iceConnectionState === "connected";
    }


    pause_playback() {
        if (this.#audio_element)
            this.#audio_element.pause();
    }

    start_playback() {
        if (this.#audio_element) {
            const playPromise = this.#audio_element.play();
            if (playPromise && typeof playPromise.then === "function") {
                playPromise.catch(err => {
                    console.warn(this.#log_prefix + "audio_element.play() failed:", err);
                });
            }
        }
    }

    //-----------------------------------------------------------------------------
    // peer connection
    //-----------------------------------------------------------------------------
    set_up_peer_connection(peer_configuration) {
        if (!peer_configuration)
            peer_configuration = this.#peer_configuration;
        this.#peer_connection = new RTCPeerConnection(peer_configuration);
        this.#set_up_pc_events();
    }

    #set_up_pc_events() {
        console.log(this.#log_prefix + "connect to peer");

        this.#peer_connection.addEventListener("track", (event) => {
            if (!!this.#inbound_stream) {
                console.warn(this.#log_prefix + "received new inbound stream, " +
                    "but we already have one. Please handel");
            }

            console.log(this.#log_prefix + "create inbound stream");
            this.#inbound_stream = event.streams[0];

            let body = document.getElementsByTagName("body")[0];
            let audio_container = body.querySelector("#audio_container");
            if (!audio_container) {
                audio_container = document.createElement("div");
                audio_container.id = "audio_container";
                body.appendChild(audio_container);
            }

            if (this.#audio_element)
                audio_container.removeChild(this.#audio_element);

            this.#audio_element = document.createElement("audio");
            this.#audio_element.id = this.#inbound_stream.id;
            this.#audio_element.crossOrigin = "anonymous";
            this.#audio_element.autoplay = false;
            this.#audio_element.volume = 1.0;
            this.#audio_element.srcObject = this.#inbound_stream;

            audio_container.appendChild(this.#audio_element);

            let track = event.track;

            track.onmute = () => {
                console.warn(this.#log_prefix + "event track.onmute. PLEASE HANDLE!", track);
            };

            track.onunmute = () => {
                console.warn(this.#log_prefix + "event track.onunmute. PLEASE HANDLE!", track);
            };

            track.onended = async () => {
                console.warn(this.#log_prefix + "event track.onended. PLEASE HANDLE!", track);
            };
        });

        this.#peer_connection.onnegotiationneeded = async (options) => {
            console.warn(this.#log_prefix + "negotiation needed. Please handel");
        };

        this.#peer_connection.onicecandidate = (event) => {
            if (event.candidate) {
                console.log(this.#log_prefix + "new ICE candidate: ", event.candidate);
                this.#send_ice_candidate(event.candidate);
            }
        };

        this.#peer_connection.oniceconnectionstatechange = () => {
            console.log(this.#log_prefix + "new ICE connection state: " +
                this.#peer_connection.iceConnectionState);
            if (this.#peer_connection.iceConnectionState === "disconnected") {
                console.warn(this.#log_prefix + "peer connection ice connection disconnected");
                this.disconnect();
            } else if (this.#peer_connection.iceConnectionState === "connected") {
                console.log(this.#log_prefix + "peer connection ice connection established");
            } else if (this.#peer_connection.iceConnectionState === "failed") {
                console.warn(this.#log_prefix + "peer connection ice connection failed");
                this.disconnect();
            }
        };

        this.#peer_connection.onicegatheringstatechange = (event) => {
            console.log(this.#log_prefix + "new ICE gathering state: " +
                event.target.iceGatheringState);
            if (event.target.iceGatheringState === "complete")
                this.#send_end_of_ice_candidates();
        };
    }

    //-----------------------------------------------------------------------------
    // outbound tone injection (TX quindar)
    //-----------------------------------------------------------------------------

    async #play_tx_tone() {
        if (this.#tx_tone_in_progress) {
            return;
        }
        if (!this.#audio_sender || !this.#audio_sender.track) {
            console.warn(this.#log_prefix + "tx tone: no audio sender available");
            return;
        }

        this.#tx_tone_in_progress = true;

        try {
            const ctx  = ov_Audio.audio_context();
            const dest = ctx.createMediaStreamDestination();

            const osc  = ctx.createOscillator();
            const gain = ctx.createGain();

            // "Dragon-ish" / Quindar-like tone
            osc.type = "sine";
            osc.frequency.value = 1969;   // Hz
            gain.gain.value = 0.4;        // 0..1

            osc.connect(gain).connect(dest);

            // Optional: let local user hear the tone too
            try {
                gain.connect(ctx.destination);
            } catch (e) {
                // ignore if not allowed
            }

            const toneTrack = dest.stream.getAudioTracks()[0];
            if (!toneTrack) {
                console.warn(this.#log_prefix + "tx tone: no audio track in tone stream");
                osc.disconnect();
                this.#tx_tone_in_progress = false;
                return;
            }

            const originalTrack = this.#audio_sender.track;
            const durationMs = 250;           // tone length
            const durationSec = durationMs / 1000;

            await this.#audio_sender.replaceTrack(toneTrack);
            console.log(this.#log_prefix + "tx tone: tone track active on sender");

            const stopTime = ctx.currentTime + durationSec;
            osc.start();
            osc.stop(stopTime);

            osc.onended = async () => {
                try {
                    await this.#audio_sender.replaceTrack(originalTrack);
                    console.log(this.#log_prefix + "tx tone: restored original mic track");
                } catch (err) {
                    console.error(this.#log_prefix + "tx tone: failed to restore mic track", err);
                }
                try {
                    osc.disconnect();
                    gain.disconnect();
                } catch (e) {}
                this.#tx_tone_in_progress = false;
            };
        } catch (err) {
            console.error(this.#log_prefix + "tx tone error:", err);
            this.#tx_tone_in_progress = false;
        }
    }

    async create_answer(offer) {
        console.log(this.#log_prefix + "received new offer: ", offer);
        console.log(this.#log_prefix + "handle new offer");

        if (!this.outbound_stream) {
            console.error(this.#log_prefix + "no stream is set");
            return;
        }

        try {
            console.log(this.#log_prefix + "set remote description");
            await this.#peer_connection.setRemoteDescription(offer);
            if (offer.type == "offer") {
                console.log(this.#log_prefix + "handle sdp offer");
                for (let track of this.#outbound_stream.stream.getTracks()) {
                    const sender = this.#peer_connection.addTrack(track, this.#outbound_stream.stream);
                    console.log(this.#log_prefix + " add track", track);

                    if (track.kind === "audio") {
                        this.#audio_sender = sender;
                        console.log(this.#log_prefix + "audio sender set", sender);
                    }
                }
                console.log(this.#log_prefix + "create answer");
                let answer = await this.#peer_connection.createAnswer();
                console.log(this.#log_prefix + "set local description");
                await this.#peer_connection.setLocalDescription(answer);
            }
        } catch (error) {
            console.error(this.#log_prefix + error);
        }
        return this.#peer_connection.localDescription;
    }

    add_ice_candidate(candidate, sdpMid, sdpMLineIndex, ufrag) {
        let candidate_wrapper = {
            candidate: candidate,
            sdpMLineIndex: sdpMLineIndex,
            sdpMid: sdpMid,
            usernameFragment: ufrag
        };
        console.log(this.#log_prefix + "add ICE Candidate to peer connection: ",
            candidate_wrapper);
        this.#peer_connection.addIceCandidate(candidate_wrapper);
    }

    //-----------------------------------------------------------------------------
    // promise to init media connection
    //-----------------------------------------------------------------------------
    #resolve_init_media;
    #init_media_successful() {
        // resolve promise of init_media_connection
        if (this.#resolve_init_media)
            this.#resolve_init_media();
    }

    async init_media_connection() {
        try {
            await new Promise(async (resolve, reject) => {
                // allow for the promise to be resolved from outside the promise
                this.#resolve_init_media = resolve;

                let offer = await ov_Vocs.request_media_connection(this.#websocket);
                if (!offer)
                    reject();
                if (!(await this.#handle_media({ detail: { message: offer } })))
                    reject();
            });
            return true;
        } catch (error) {
            return false;
        }
    }

    //-----------------------------------------------------------------------------
    // outbound media stream
    //-----------------------------------------------------------------------------
    static async create_local_media_stream() {
        console.log("(ov_WebRTC): create outbound media stream");
        if (typeof DEBUG_USE_MEDIA_STREAM_FROM_FILE !== "undefined" &&
            DEBUG_USE_MEDIA_STREAM_FROM_FILE) {
            ov_WebRTC.#local_stream = ov_Audio.media_stream_from_file(DEBUG_MEDIA_STREAM_FILE);
        } else
            ov_WebRTC.#local_stream = await ov_Audio.media_stream_from_microphone();

        const audio_tracks = ov_WebRTC.#local_stream.stream.getAudioTracks();
        if (audio_tracks.length > 0)
            console.log(`(ov_WebRTC): Using audio device: ${audio_tracks[0].label}`);
    }

    mute_outbound_stream(mute) {
    console.log(this.#log_prefix + "IS MUTE", mute);

    const wasMuted = this.#is_muted;
    this.#is_muted = !!mute;

    // Only do the tone on a rising edge, and only if user enabled it
    const txToneEnabled =
        (typeof window !== "undefined") && window.ov_tx_quindar_enabled === true;

    if (txToneEnabled && wasMuted && !mute) {
        // muted -> unmuted and tone enabled
        this.#play_tx_tone();
    }

    this.#outbound_stream.mute = mute;
}


    // ----------------------------------------------------------------------------
    // send media events to signaling server
    // ----------------------------------------------------------------------------
    async #send_ice_candidate(candidate) {
        if (candidate.candidate)
            return await ov_Vocs.send_ice_candidate(candidate.candidate,
                candidate.sdpMid, candidate.sdpMLineIndex, candidate.usernameFragment,
                this.#websocket);
        return false;
    }

    async #send_end_of_ice_candidates() {
        return await ov_Vocs.send_end_of_ice_candidates(this.#websocket);
    }

    // ----------------------------------------------------------------------------
    // handel incoming events (server or peer messages)
    // ----------------------------------------------------------------------------
    #handle_ice_candidate(event) {
        if (!event.detail.sender.client) //server message
            this.add_ice_candidate(event.detail.message.candidate, event.detail.message.sdpMid,
                event.detail.message.sdpMLineIndex, event.detail.message.ufrag);
    }

    #handle_end_of_ice_candidates(event) {
        if (!event.detail.sender.client) //server message
            this.add_ice_candidate("", event.detail.message.sdpMid, event.detail.message.sdpMLineIndex,
                event.detail.message.ufrag);
    }

    #handle_media_ready(event) {
        if (!event.detail.sender.client) { //server message
            console.log(this.#log_prefix + "media connection is established");
            this.#init_media_successful();
        }
    }

    async #handle_media(event) {
        if (!event.detail.sender || (event.detail.sender && !event.detail.sender.client)) { //server or local message
            if (event.detail.message.type === "offer") {
                if (!this.is_connected)
                    this.set_up_peer_connection();
                let answer = await this.create_answer(event.detail.message);
                return await ov_Vocs.send_media_answer(answer.sdp, this.#websocket);
            }
        }
        return true;
    }

    //-----------------------------------------------------------------------------
    // disconnect handling
    //-----------------------------------------------------------------------------
    disconnect() {
        console.log(this.#log_prefix + "close WebRTC connection");
        this.#peer_connection.close();
        if (!!this.#disconnect_handler)
            this.#disconnect_handler();
    }

    #disconnect_handler;
    on_disconnect(disconnect_handler) {
        this.#disconnect_handler = disconnect_handler;
    }

    //-----------------------------------------------------------------------------
    // logging
    //-----------------------------------------------------------------------------
    get #log_prefix() {
        return "(" + this.#websocket.server_name + " WebRTC) ";
    }
}
